# sfda-cosmetic-sync-worker-
sfda-cosmetic-sync-worker


# SFDA Cosmetic Mirror — Resilient Sync Worker

محرك سحب ومزامنة مستقل بلغة Python يسحب القوائم الخمس لمكونات منتجات التجميل من موقع هيئة الغذاء والدواء السعودية (SFDA) ويرسلها على دفعات موقّعة بـ `HMAC-SHA256` إلى إضافة ووردبريس، مع دعم الاستئناف الذري (Checkpoint & Resume) وتوليد نسخة احتياطية بصيغة JSON.

## إعداد أسرار GitHub Secrets
داخل المستودع الخاص على GitHub افتح: **Settings → Secrets and variables → Actions → New repository secret** وأضف المتغيرات الثلاثة التالية:

1. `WP_SYNC_BASE_URL` — مثال: `https://astc-arabia.com/wp-json/sfda-mirror/v1/sync`
2. `WP_SFDA_API_KEY` — مفتاح الربط الظاهر في لوحة تحكم الإضافة.
3. `WP_SFDA_HMAC_SECRET` — المفتاح السري لتوقيع HMAC الظاهر في لوحة تحكم الإضافة.

## التشغيل المحلي السريع (اختبار سحب بدون ووردبريس)
```bash
pip install -r requirements.txt
python -m src.main --lists uv_filters --dry-run
