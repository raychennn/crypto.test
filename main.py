import os
import asyncio
import logging
import pytz
from datetime import datetime
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from io import BytesIO

from data_loader import DataLoader
from screener import CryptoScreener
import config

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID") 

application = None

async def run_screener_logic(context_chat_id=None):
    target_chat_id = context_chat_id if context_chat_id else CHAT_ID
    
    if not target_chat_id:
        logger.warning("No Chat ID provided, skipping message send.")
        return

    loader = DataLoader()
    try:
        # 這裡會跑很久 (Semaphore 限制速度)，發個訊息通知開始
        if context_chat_id:
            try:
                await application.bot.send_message(chat_id=target_chat_id, text="🔍 Scanning market (throttled mode)...")
            except:
                pass

        data_map, btc_data = await loader.get_all_data()
        screener = CryptoScreener(data_map, btc_data)
        results = screener.run()
        
        # [關鍵修正] 使用純文字，不依賴 Markdown
        msg = f"=== Screening Result ({datetime.now().strftime('%H:%M')}) ===\n"
        msg += f"Top {len(results)} Candidates\n\n"
        
        for r in results:
            icon = "🚀" if r['bucket'] == 'Leader' else "⚡" if r['bucket'] == 'PowerPlay' else "🔄"
            # 移除 ** 等特殊符號
            msg += f"{icon} {r['symbol']} (RS:{r['rs_rank']})\n"
            msg += f"   Type: {r['setup']} | Score: {r['score']}\n"
        
        if not results:
            msg = "No assets passed the strict criteria this round."

        # 移除 parse_mode 參數，使用預設純文字
        await application.bot.send_message(chat_id=target_chat_id, text=msg)

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
    await update.message.reply_text("⏳ Request received. Starting scan...")
    await run_screener_logic(context_chat_id=update.effective_chat.id)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(f"Bot is running! Your Chat ID is: {chat_id}\nUse /now to screen instantly.")

async def post_init(app: Application):
    logger.info("Setting up scheduler in post_init...")
    scheduler = AsyncIOScheduler(timezone=pytz.timezone(config.TIMEZONE))
    
    times = ["03:59", "07:59", "11:59", "15:59", "19:59", "23:59"]
    for t in times:
        h, m = t.split(":")
        scheduler.add_job(scheduled_job, 'cron', hour=h, minute=m)
    
    scheduler.start()
    logger.info(f"Scheduler started with {len(times)} jobs.")

def main():
    global application
    application = Application.builder().token(TOKEN).post_init(post_init).build()
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("now", cmd_now))
    
    logger.info("Bot is starting polling...")
    application.run_polling()

if __name__ == "__main__":
    main()
