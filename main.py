import os
import asyncio
import logging
import pytz
from datetime import datetime
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from io import BytesIO

# 確保載入你的模組
from data_loader import DataLoader
from screener import CryptoScreener
import config

# Setup Logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Env Variables
TOKEN = os.getenv("TELEGRAM_TOKEN")
# 建議設定預設 Chat ID，方便除錯
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID") 

# 全域變數，讓 application 可以被存取
application = None

async def run_screener_logic(context_chat_id=None):
    """執行篩選邏輯並發送結果"""
    # 這裡使用 application.bot 來發送訊息
    target_chat_id = context_chat_id if context_chat_id else CHAT_ID
    
    if not target_chat_id:
        logger.warning("No Chat ID provided, skipping message send.")
        return

    loader = DataLoader()
    try:
        data_map, btc_data = await loader.get_all_data()
        screener = CryptoScreener(data_map, btc_data)
        results = screener.run()
        
        # 1. Text Message
        msg = f"📊 **Screening Result** ({datetime.now().strftime('%H:%M')})\n"
        msg += f"Top {len(results)} Candidates\n\n"
        
        for r in results:
            icon = "🚀" if r['bucket'] == 'Leader' else "⚡" if r['bucket'] == 'PowerPlay' else "🔄"
            msg += f"{icon} **{r['symbol']}** (RS:{r['rs_rank']})\n"
            msg += f"   Type: {r['setup']} | Score: {r['score']}\n"
        
        if not results:
            msg = "No assets passed the strict criteria this round."

        from telegram.constants import ParseMode
        await application.bot.send_message(chat_id=target_chat_id, text=msg, parse_mode=ParseMode.MARKDOWN)

        # 2. TradingView TXT
        if results:
            txt_content = ",".join([f"BINANCE:{r['symbol']}" for r in results])
            file_obj = BytesIO(txt_content.encode())
            file_obj.name = f"watchlist_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
            await application.bot.send_document(chat_id=target_chat_id, document=file_obj)
        
    except Exception as e:
        logger.error(f"Error in job: {e}", exc_info=True)
        if target_chat_id:
            try:
                await application.bot.send_message(chat_id=target_chat_id, text=f"⚠️ Error: {str(e)}")
            except:
                pass

async def scheduled_job():
    logger.info("Running scheduled screening...")
    await run_screener_logic()

async def cmd_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Running screener now... please wait (approx 1-2 mins).")
    await run_screener_logic(context_chat_id=update.effective_chat.id)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(f"Bot is running! Your Chat ID is: `{chat_id}`\nUse /now to screen instantly.", parse_mode='Markdown')

async def post_init(app: Application):
    """
    這個函數會在 Bot 啟動且 Event Loop 準備好之後執行。
    我們在這裡啟動排程器，避免 'no running event loop' 錯誤。
    """
    logger.info("Setting up scheduler in post_init...")
    
    scheduler = AsyncIOScheduler(timezone=pytz.timezone(config.TIMEZONE))
    
    # 設定排程時間
    times = ["03:59", "07:59", "11:59", "15:59", "19:59", "23:59"]
    for t in times:
        h, m = t.split(":")
        scheduler.add_job(scheduled_job, 'cron', hour=h, minute=m)
    
    scheduler.start()
    logger.info(f"Scheduler started with {len(times)} jobs.")

def main():
    global application
    
    # 建立 Application，並註冊 post_init
    # post_init 參數非常關鍵，它確保了順序的正確性
    application = Application.builder().token(TOKEN).post_init(post_init).build()

    # Handlers
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("now", cmd_now))
    
    logger.info("Bot is starting polling...")
    application.run_polling()

if __name__ == "__main__":
    main()
