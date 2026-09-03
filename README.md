# 🐂 Bull Hunter v2 — بک‌تست واقعی روی GitHub Actions

این ریپو یک بک‌تست **واقعی** برای استراتژی Bull Hunter v2 اجرا می‌کند — با داده واقعی از
Binance Public API — روی زیرساخت رایگان GitHub Actions.

## چرا GitHub Actions؟

بسیاری از محیط‌های sandbox (از جمله دستیارهای هوش مصنوعی) به API صرافی‌ها دسترسی
مستقیم ندارند. اما وقتی یک workflow در GitHub Actions اجرا می‌شود، سرور اجراکننده
(GitHub-hosted runner) دسترسی کامل به اینترنت دارد — یعنی می‌تواند واقعاً به
`api.binance.com` وصل شود و کندل‌های واقعی را بگیرد.

## راه‌اندازی (۳ قدم)

### ۱. ساخت ریپو
یک ریپوی جدید در GitHub بسازید (public یا private) و همه فایل‌های این پوشه را
در آن push کنید:

```bash
git init
git add .
git commit -m "Bull Hunter v2 backtest setup"
git branch -M main
git remote add origin https://github.com/<username>/<repo-name>.git
git push -u origin main
```

### ۲. اجرای دستی بک‌تست
به تب **Actions** ریپو بروید → workflow با نام **"🐂 Bull Hunter v2 — بک‌تست واقعی"**
را انتخاب کنید → دکمه **Run workflow** را بزنید. می‌توانید پارامترها را قبل از اجرا
تنظیم کنید:

| پارامتر | توضیح | پیش‌فرض |
|---|---|---|
| `days` | چند روز گذشته بک‌تست شود | 14 |
| `interval` | تایم‌فریم کندل (1m/5m/15m/1h) | 5m |
| `symbols` | چند ارز از لیست Top 200 (برای تست سریع کمتر بدهید) | 200 |
| `min_change` | حداقل رشد ٪ برای سیگنال اولیه | 1.5 |
| `min_tests` | حداقل تعداد تست پاس‌شده از ۸ | 7 |

⚠️ **نکته مهم:** با `symbols=200` و `interval=1m`، اجرا ممکن است به دلیل تعداد
درخواست‌های API طولانی شود (تا ۹۰ دقیقه، طبق تنظیم `timeout-minutes`). برای
تست سریع اول با `symbols=20` و `interval=15m` شروع کنید.

### ۳. دیدن نتایج
بعد از پایان اجرا (چند دقیقه تا حدود یک ساعت، بسته به تنظیمات):
- در همان صفحه Actions → روی اجرای انجام‌شده کلیک کنید → بخش **Artifacts** →
  فایل `backtest-results-N` را دانلود کنید (شامل `summary.json`, `trades.json`, `report.md`)
- اگر `permissions: contents: write` فعال باشد، نتایج در پوشه‌های `results/` و
  `history/` مستقیماً به ریپو commit می‌شوند — یعنی می‌توانید لینک خام آن‌ها را
  در مرورگر یا با دستیار هوش مصنوعی بخوانید:
  ```
  https://raw.githubusercontent.com/<username>/<repo-name>/main/results/summary.json
  ```

## اجرای محلی (روی سیستم خودتان)

```bash
pip install -r requirements.txt
python src/backtest_real.py --days 14 --interval 5m --symbols 200
```

## فایل‌های این ریپو

```
.
├── .github/workflows/backtest.yml   ← تعریف GitHub Action
├── src/backtest_real.py             ← موتور اصلی بک‌تست (۸ تست + ATR trailing)
├── requirements.txt                 ← وابستگی‌های Python
├── results/                         ← نتایج آخرین اجرا (تولید خودکار)
└── history/                         ← آرشیو نتایج هر اجرا (تولید خودکار)
```

## چطور کار می‌کند

اسکریپت `src/backtest_real.py`:

1. برای هر یک از ۲۰۰ ارز، کندل‌های واقعی OHLCV را از Binance Public API می‌گیرد
   (با pagination خودکار برای بازه‌های طولانی)
2. دقیقاً همان ۸ تست استراتژی v2 را محاسبه می‌کند:
   حجم نسبی، RSI(7)، MACD Histogram، Bollinger Breakout، VWAP، فیلتر تکرار،
   EMA Cross، ATR Squeeze
3. برای هر سیگنال تأیید‌شده، یک معامله شبیه‌سازی‌شده باز می‌کند و با
   منطق واقعی ATR Trailing Stop آن را می‌بندد
4. نتایج را در سه فرمت ذخیره می‌کند: `summary.json` (خلاصه آماری)،
   `trades.json` (جزئیات هر معامله)، `report.md` (گزارش خوانا)

هیچ عددی در این فرآیند شبیه‌سازی یا حدس زده نمی‌شود — همه چیز از کندل‌های
واقعی Binance محاسبه می‌شود.

## محدودیت‌ها

- داده Binance شامل معاملات spot است؛ نتایج فرضی هستند و شامل کارمزد صرافی،
  اسلیپیج، یا محدودیت نقدینگی واقعی نیستند.
- این یک بک‌تست است، نه تضمین عملکرد آینده.
- نرخ محدودیت (rate limit) عمومی Binance ممکن است روی بازه‌های خیلی طولانی
  (`days` بزرگ + `interval=1m` + `symbols=200`) باعث کندی یا خطای موقت شود؛
  اسکریپت به‌صورت خودکار retry نمی‌کند، فقط آن نماد را رد می‌کند و ادامه می‌دهد.
