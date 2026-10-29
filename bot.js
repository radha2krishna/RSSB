/**
 * RSSB Instant Multi-Category Telegram Bot with PDF Watermarking
 * -------------------------------------------------------------
 * • 1-Second Multi-Category Prober across 8 RSSB storage folders
 * • Dedicated separate download pipeline & independent manual probe sockets
 * • Pure multipart/form-data Buffer upload with explicit Content-Length (Never hangs Telegram API)
 * • Channel Support: Listens to channel_post, my_chat_member, and forwarded posts
 * • Dynamic Channel Binding: /bind command + auto-detection on admin add or message forward
 * • Automatic PDF Watermarking & Clickable Banners via pdf-lib
 * • 50 MB Telegram upload safeguard (auto-switches to link if > 49MB)
 * • Inline Action Buttons (Direct Link + 1-Tap Share to Friends)
 * • Sequential dispatch queue to avoid Telegram 429 Rate Limits
 * • Memory-bounded rolling window (Zero memory leak on months-long runs)
 * • Global crash guard (uncaughtException & unhandledRejection handlers)
 * • Interactive Admin/User commands (/status, /check, /categories, /ping, /testsend, /bind, /id)
 * • Built-in HTTP health check server for Koyeb / Render on port 8000
 */
'use strict';

const http = require('http');
const https = require('https');
const crypto = require('crypto');

// ── OPTIONAL PDF-LIB LOADING (For PDF Watermarking) ───────────────────────
let PDFLib = null;
try {
    PDFLib = require('pdf-lib');
} catch (e) {
    // Falls back gracefully to original unmodified PDF if pdf-lib is absent
}

// ── ENVIRONMENT VARIABLES ─────────────────────────────────────────────────
const PORT = process.env.PORT || 8000;
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const INITIAL_CHAT_IDS = process.env.CHAT_IDS
    ? process.env.CHAT_IDS.split(',').map(s => s.trim()).filter(Boolean)
    : (CHAT_ID ? [CHAT_ID.trim()] : []);

// Set of active destination chats (can be expanded dynamically via /bind)
const boundChats = new Set(INITIAL_CHAT_IDS);

// Watermark & Branding Config
const WATERMARK_TEXT = process.env.WATERMARK_TEXT || '';
const BANNER_TEXT = process.env.BANNER_TEXT || '';
const BANNER_URL = process.env.BANNER_URL || '';

// Optional: Comma-separated list of categories to exclude (e.g. tender_item)
const EXCLUDE_CATEGORIES = (process.env.EXCLUDE_CATEGORIES || '')
    .split(',')
    .map(s => s.trim().toLowerCase())
    .filter(Boolean);

// Monitored RSSB Storage Categories
const ALL_CATEGORIES = [
    { key: 'result_item', label: 'Result & Cut-Off Marks', icon: '🏆', hashtag: '#RSSB #Result #CutOff' },
    { key: 'news_item', label: 'Latest News & Alert', icon: '🚨', hashtag: '#RSSB #Update #ExamNews' },
    { key: 'pressnote_item', label: 'Press Note', icon: '📢', hashtag: '#RSSB #PressNote #DateAlert' },
    { key: 'answerkey_item', label: 'Answer Key', icon: '🔑', hashtag: '#RSSB #AnswerKey #Objection' },
    { key: 'questionpaper_item', label: 'Master Question Paper', icon: '📝', hashtag: '#RSSB #QuestionPaper' },
    { key: 'advertisement_item', label: 'Direct Recruitment', icon: '💼', hashtag: '#RSSB #Vacancy #GovtJob' },
    { key: 'admitcard_item', label: 'Admit Card', icon: '🎫', hashtag: '#RSSB #AdmitCard #ExamCity' },
    { key: 'guideline_item', label: 'Exam Guideline', icon: '📌', hashtag: '#RSSB #Instructions' },
    { key: 'tender_item', label: 'Tenders', icon: '📁', hashtag: '#RSSB #Tender' }
];

// Filter out excluded categories
const CATEGORIES = ALL_CATEGORIES.filter(c => !EXCLUDE_CATEGORIES.includes(c.key));

const RSSB_BASE = 'https://rssb.rajasthan.gov.in';

// ── HTTPS AGENTS ──────────────────────────────────────────────────────────
// 1. Dedicated Agent for 1-second routine HEAD scanning
const scanAgent = new https.Agent({
    secureOptions: crypto.constants.SSL_OP_LEGACY_SERVER_CONNECT || 0x4,
    rejectUnauthorized: false,
    keepAlive: true,
    maxSockets: 100,
    maxFreeSockets: 50
});

// 2. Dedicated Agent for downloading full PDF files
const downloadAgent = new https.Agent({
    secureOptions: crypto.constants.SSL_OP_LEGACY_SERVER_CONNECT || 0x4,
    rejectUnauthorized: false,
    keepAlive: true,
    maxSockets: 20,
    maxFreeSockets: 10
});

// 3. Dedicated Agent for manual /check queries (Fresh sockets to prevent stale resets)
const manualAgent = new https.Agent({
    secureOptions: crypto.constants.SSL_OP_LEGACY_SERVER_CONNECT || 0x4,
    rejectUnauthorized: false,
    keepAlive: false
});

// Tracker & Statistics
const sentHits = new Set();
const recentChecked = new Map();

let stats = {
    startedAt: Date.now(),
    totalChecks: 0,
    totalHits: 0,
    sentCount: 0,
    lastHit: null
};

// Known past IDs to avoid re-alerting old notices on fresh start
const KNOWN_PAST = [
    'result_item:1793274315', 'result_item:1793125170', 'result_item:1793125287', 'result_item:1793125321',
    'result_item:1792226524', 'result_item:1792226581', 'result_item:1792226553',
    'news_item:1793188157', 'news_item:1793099273', 'news_item:1792756589',
    'pressnote_item:1782361794', 'answerkey_item:1790683754', 'questionpaper_item:1789712214'
];
KNOWN_PAST.forEach(k => sentHits.add(k));

const nowEpoch = () => Math.floor(Date.now() / 1000);

function epochToIST(ts) {
    const d = new Date((Number(ts) + 19800) * 1000);
    const p = n => String(n).padStart(2, '0');
    return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())} ${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:${p(d.getUTCSeconds())} IST`;
}

function fmtUptime(ms) {
    const s = Math.floor(ms / 1000);
    const d = Math.floor(s / 86400);
    const h = Math.floor((s % 86400) / 3600);
    const m = Math.floor((s % 3600) / 60);
    const sec = s % 60;
    return `${d > 0 ? d + 'd ' : ''}${h}h ${m}m ${sec}s`;
}

function escapeHtml(str) {
    if (!str) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

// ── Ultra-Fast Single Category HEAD Probe (Loop Routine) ─────────────────
function checkCategoryPdf(catKey, epochId) {
    return new Promise((resolve) => {
        const req = https.request({
            hostname: 'rssb.rajasthan.gov.in',
            port: 443,
            path: `/storage/${catKey}/${epochId}.pdf`,
            method: 'HEAD',
            agent: scanAgent,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': `${RSSB_BASE}/`,
                'Connection': 'keep-alive'
            },
            timeout: 5000
        }, (res) => {
            const hit = res.statusCode === 200 || res.statusCode === 206;
            const size = parseInt(res.headers['content-length'] || '0', 10);
            res.resume();
            resolve({ catKey, epochId, hit, size, status: res.statusCode });
        });

        req.on('error', () => resolve({ catKey, epochId, hit: false, size: 0, status: 0 }));
        req.on('timeout', () => { req.destroy(); resolve({ catKey, epochId, hit: false, size: 0, status: 0 }); });
        req.end();
    });
}

// ── Manual Category Probe (For /check command with generous timeout) ──────
function probeManualCategoryPdf(catKey, epochId, timeoutMs = 15000) {
    return new Promise((resolve) => {
        const req = https.request({
            hostname: 'rssb.rajasthan.gov.in',
            port: 443,
            path: `/storage/${catKey}/${epochId}.pdf`,
            method: 'HEAD',
            agent: manualAgent,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': `${RSSB_BASE}/`
            },
            timeout: timeoutMs
        }, (res) => {
            const hit = res.statusCode === 200 || res.statusCode === 206;
            const size = parseInt(res.headers['content-length'] || '0', 10);
            res.resume();
            resolve({ catKey, epochId, hit, size, status: res.statusCode });
        });

        req.on('error', (err) => resolve({ catKey, epochId, hit: false, size: 0, status: 0, error: err.message }));
        req.on('timeout', () => { req.destroy(); resolve({ catKey, epochId, hit: false, size: 0, status: 0, error: 'TIMEOUT' }); });
        req.end();
    });
}

// ── Fast & Resilient Download of Hit PDF (With Auto-Retry) ─────────────────
function downloadPdfOnce(catKey, epochId, timeoutMs = 35000) {
    return new Promise((resolve, reject) => {
        let isDone = false;
        const timer = setTimeout(() => {
            if (!isDone) {
                isDone = true;
                req.destroy(new Error(`Download timeout after ${timeoutMs / 1000}s`));
            }
        }, timeoutMs);

        const req = https.get({
            hostname: 'rssb.rajasthan.gov.in',
            port: 443,
            path: `/storage/${catKey}/${epochId}.pdf`,
            agent: downloadAgent,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': `${RSSB_BASE}/`,
                'Accept': '*/*',
                'Connection': 'keep-alive'
            }
        }, (res) => {
            if (res.statusCode !== 200 && res.statusCode !== 206) {
                clearTimeout(timer);
                isDone = true;
                res.resume();
                return reject(new Error(`HTTP ${res.statusCode}`));
            }
            const chunks = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => {
                if (!isDone) {
                    clearTimeout(timer);
                    isDone = true;
                    resolve(Buffer.concat(chunks));
                }
            });
            res.on('error', (e) => {
                if (!isDone) {
                    clearTimeout(timer);
                    isDone = true;
                    reject(e);
                }
            });
        });

        req.on('error', (e) => {
            if (!isDone) {
                clearTimeout(timer);
                isDone = true;
                reject(e);
            }
        });
    });
}

// Wrapper with 1 retry
async function downloadPdf(catKey, epochId) {
    try {
        return await downloadPdfOnce(catKey, epochId, 30000);
    } catch (e1) {
        console.warn(`[RETRY] First attempt for ${catKey}/${epochId} failed (${e1.message}), retrying...`);
        return await downloadPdfOnce(catKey, epochId, 45000);
    }
}

// ── Extract Original Document Title (/Title metadata) ─────────────────────
function extractPdfTitle(buffer) {
    try {
        const tail = buffer.subarray(Math.max(0, buffer.length - 12288)).toString('latin1');
        const m = tail.match(/\/Title\s*\(([^)]+)\)/);
        if (m && m[1]) {
            return m[1].replace(/\\([()\\])/g, '$1').trim();
        }
    } catch {}
    return null;
}

// ── PDF Watermark & Branding (Diagonal Text + Top/Bottom Banners) ─────────
async function brandPdf(pdfBytes) {
    if (!PDFLib || !WATERMARK_TEXT) {
        return { brandedBytes: pdfBytes, modified: false };
    }
    try {
        const { PDFDocument, rgb, degrees, StandardFonts } = PDFLib;
        const pdfDoc = await PDFDocument.load(pdfBytes, { ignoreEncryption: true });
        const font = await pdfDoc.embedFont(StandardFonts.HelveticaBold);
        const pages = pdfDoc.getPages();

        // Sanitize watermark text to Latin-1 range so Standard Helvetica never throws
        const watermarkText = WATERMARK_TEXT.replace(/[^\x20-\x7E\xA0-\xFF]/g, ' ').trim();
        const bannerText = (BANNER_TEXT || WATERMARK_TEXT).replace(/[^\x20-\x7E\xA0-\xFF]/g, ' ').trim();

        if (!watermarkText) {
            return { brandedBytes: pdfBytes, modified: false };
        }

        for (const page of pages) {
            const { width, height } = page.getSize();

            // 1. Diagonal Watermark Grid
            const wFontSize = Math.max(16, Math.min(width, height) * 0.055);
            const cols = 3;
            const rows = 4;
            for (let c = 0; c < cols; c++) {
                for (let r = 0; r < rows; r++) {
                    page.drawText(watermarkText, {
                        x: (width / cols) * c + 25,
                        y: (height / rows) * r + 25,
                        size: wFontSize,
                        font,
                        color: rgb(0.72, 0.72, 0.72),
                        opacity: 0.28,
                        rotate: degrees(45),
                    });
                }
            }

            // 2. Clickable Top/Bottom Banners
            if (bannerText) {
                const bannerHeight = 18;
                const margin = 6;
                const bFontSize = 9;
                const textWidth = font.widthOfTextAtSize(bannerText, bFontSize);

                // Top Banner
                const topY = height - margin - bannerHeight;
                page.drawRectangle({
                    x: margin,
                    y: topY,
                    width: width - (margin * 2),
                    height: bannerHeight,
                    color: rgb(0.08, 0.16, 0.32),
                    opacity: 0.92,
                });
                page.drawText(bannerText, {
                    x: Math.max(margin + 5, (width - textWidth) / 2),
                    y: topY + 4.5,
                    size: bFontSize,
                    font,
                    color: rgb(1, 1, 1),
                });

                // Bottom Banner
                const bottomY = margin;
                page.drawRectangle({
                    x: margin,
                    y: bottomY,
                    width: width - (margin * 2),
                    height: bannerHeight,
                    color: rgb(0.08, 0.16, 0.32),
                    opacity: 0.92,
                });
                page.drawText(bannerText, {
                    x: Math.max(margin + 5, (width - textWidth) / 2),
                    y: bottomY + 4.5,
                    size: bFontSize,
                    font,
                    color: rgb(1, 1, 1),
                });
            }
        }

        const brandedPdfBytes = await pdfDoc.save();
        return { brandedBytes: Buffer.from(brandedPdfBytes), modified: true };
    } catch (e) {
        console.error('Watermark error (sending original):', e.message);
        return { brandedBytes: pdfBytes, modified: false };
    }
}

// ── Pure-Buffer Multipart Payload Builder (With Explicit Content-Length) ──
function buildMultipartPayload(fields, fileField) {
    const boundary = '----WebKitFormBoundary' + crypto.randomBytes(16).toString('hex');
    const chunks = [];

    for (const [key, value] of Object.entries(fields)) {
        if (value === undefined || value === null) continue;
        chunks.push(Buffer.from(
            `--${boundary}\r\nContent-Disposition: form-data; name="${key}"\r\n\r\n${value}\r\n`
        ));
    }

    if (fileField && fileField.buffer) {
        const filename = fileField.filename || 'document.pdf';
        const contentType = fileField.contentType || 'application/pdf';
        chunks.push(Buffer.from(
            `--${boundary}\r\nContent-Disposition: form-data; name="${fileField.name || 'document'}"; filename="${filename}"\r\nContent-Type: ${contentType}\r\n\r\n`
        ));
        chunks.push(Buffer.isBuffer(fileField.buffer) ? fileField.buffer : Buffer.from(fileField.buffer));
        chunks.push(Buffer.from('\r\n'));
    }

    chunks.push(Buffer.from(`--${boundary}--\r\n`));
    const body = Buffer.concat(chunks);

    return {
        body,
        headers: {
            'Content-Type': `multipart/form-data; boundary=${boundary}`,
            'Content-Length': String(body.length)
        }
    };
}

// ── Rate-Limit Queue (Prevents Telegram 429 Errors) ───────────────────────
const dispatchQueue = [];
let isQueueProcessing = false;

function enqueueDispatch(task) {
    dispatchQueue.push(task);
    if (!isQueueProcessing) processDispatchQueue();
}

async function processDispatchQueue() {
    isQueueProcessing = true;
    while (dispatchQueue.length > 0) {
        const task = dispatchQueue.shift();
        try {
            await task();
        } catch (e) {
            console.error('Dispatch queue task error:', e.message);
        }
        // 1.2-second pause between posts to stay well within Telegram limits
        await new Promise(r => setTimeout(r, 1200));
    }
    isQueueProcessing = false;
}

// ── Send Clean PDF Message to Telegram (Epoch ID Hidden) ──────────────────
async function sendPdfToChat(chatId, cat, epochId, rawBuffer) {
    if (!BOT_TOKEN || !chatId) {
        console.warn(`[TELEGRAM] Skipping send: BOT_TOKEN or chatId missing.`);
        return { success: false, error: 'Missing BOT_TOKEN or chatId' };
    }

    const docTitle = extractPdfTitle(rawBuffer);
    const sizeBytes = rawBuffer.length;
    const sizeKB = Math.round(sizeBytes / 1024);
    const sizeMB = (sizeBytes / (1024 * 1024)).toFixed(1);
    const pdfUrl = `${RSSB_BASE}/storage/${cat.key}/${epochId}.pdf`;
    const dateStr = epochToIST(epochId);

    // 1. Clean caption (NO raw epoch ID shown)
    let caption = `${cat.icon} <b>RSSB Official Notification</b>\n\n`
                + `📋 <b>Category:</b> ${escapeHtml(cat.label)}\n`;

    if (docTitle) {
        caption += `📄 <b>Notice:</b> <code>${escapeHtml(docTitle)}</code>\n`;
    }

    caption += `📅 <b>Published:</b> ${dateStr}\n`
            + `📦 <b>File Size:</b> ${sizeBytes > 1024 * 1024 ? sizeMB + ' MB' : sizeKB + ' KB'}\n\n`
            + `${cat.hashtag}`;

    // 2. Interactive Inline Buttons
    const inlineButtons = [
        [{ text: '📥 Official Download Link', url: pdfUrl }]
    ];
    if (BANNER_URL) {
        inlineButtons[0].push({ text: '📢 Join Channel', url: BANNER_URL });
    }
    const shareText = encodeURIComponent(`🚨 RSSB ${cat.label} Released!\n📄 ${docTitle || 'Check Notice'}\n🔗 ${pdfUrl}`);
    inlineButtons.push([{ text: '📲 Share to Friends', url: `https://t.me/share/url?url=${encodeURIComponent(pdfUrl)}&text=${shareText}` }]);

    const replyMarkup = { inline_keyboard: inlineButtons };

    // 3. 50 MB Safeguard: Telegram rejects files > 50MB
    const MAX_TELEGRAM_FILE_BYTES = 49 * 1024 * 1024; // 49 MB
    if (sizeBytes > MAX_TELEGRAM_FILE_BYTES) {
        console.log(`[SAFEGUARD] File exceeds 49MB (${sizeMB}MB), sending direct link card.`);
        try {
            await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    chat_id: String(chatId),
                    text: caption + `\n\n⚠ <i>Notice file is ${sizeMB}MB (exceeds Telegram 50MB limit). Download directly via the button below:</i>`,
                    parse_mode: 'HTML',
                    reply_markup: replyMarkup
                }),
                signal: AbortSignal.timeout(20000)
            });
            stats.sentCount++;
            return { success: true };
        } catch (e) {
            return { success: false, error: e.message };
        }
    }

    // 4. Apply Watermarking
    const { brandedBytes, modified } = await brandPdf(rawBuffer);
    const finalBytes = brandedBytes;
    let filename = docTitle ? docTitle.replace(/[\\/:*?"<>|\r\n]/g, '_').trim() : `RSSB_${cat.label.replace(/\s+/g, '_')}.pdf`;
    if (!filename.toLowerCase().endsWith('.pdf')) filename += '.pdf';

    try {
        const payload = buildMultipartPayload({
            chat_id: String(chatId),
            caption: caption,
            parse_mode: 'HTML',
            reply_markup: JSON.stringify(replyMarkup)
        }, {
            name: 'document',
            filename: filename,
            contentType: 'application/pdf',
            buffer: finalBytes
        });

        console.log(`[TELEGRAM] Uploading ${Math.round(payload.body.length / 1024)}KB (${filename}) to ${chatId}...`);
        const res = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
            method: 'POST',
            headers: payload.headers,
            body: payload.body,
            signal: AbortSignal.timeout(60000)
        });

        const data = await res.json();
        if (!data.ok) {
            throw new Error(`Telegram error: ${data.description || 'Unknown error'}`);
        }

        stats.sentCount++;
        console.log(`[TELEGRAM] ✅ ${modified ? 'Branded' : 'Original'} "${filename}" sent to ${chatId}`);
        return { success: true, messageId: data.result?.message_id };
    } catch (err) {
        console.error(`[TELEGRAM ERROR] ${chatId}: ${err.message}`);
        // Fallback: Send alert text card with direct link
        try {
            await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    chat_id: String(chatId),
                    text: caption + `\n\n⚠ <i>PDF upload failed (${escapeHtml(err.message)}). Tap button below to open document:</i>`,
                    parse_mode: 'HTML',
                    reply_markup: replyMarkup
                }),
                signal: AbortSignal.timeout(15000)
            });
        } catch {}
        return { success: false, error: err.message };
    }
}

function broadcastHit(cat, epochId, pdfBuffer) {
    const targets = Array.from(boundChats).filter(Boolean);
    if (targets.length === 0) {
        console.warn('[BROADCAST] No bound channels or chats configured! Send /bind in your Telegram channel or set CHAT_ID.');
        return;
    }
    for (const cid of targets) {
        enqueueDispatch(() => sendPdfToChat(cid, cat, epochId, pdfBuffer));
    }
}

// ── Immediate Hit Handler ─────────────────────────────────────────────────
async function onHitDetected(cat, epochId, size) {
    const hitKey = `${cat.key}:${epochId}`;
    if (sentHits.has(hitKey)) return;
    sentHits.add(hitKey);

    stats.totalHits++;
    stats.lastHit = { cat: cat.label, date: epochToIST(epochId), size };
    console.log(`\n======================================================`);
    console.log(`★ INSTANT HIT! [${cat.label}] at ${epochToIST(epochId)} [${Math.round(size / 1024)}KB]`);
    console.log(`Downloading and dispatching to Telegram queue...`);

    try {
        const buf = await downloadPdf(cat.key, epochId);
        broadcastHit(cat, epochId, buf);
    } catch (e) {
        console.error(`Download failed for ${cat.key}/${epochId} (${e.message}), sending link alert fallback...`);
        // If binary download fails, send alert with direct link so channel still gets instant update!
        const targets = Array.from(boundChats).filter(Boolean);
        const pdfUrl = `${RSSB_BASE}/storage/${cat.key}/${epochId}.pdf`;
        const caption = `${cat.icon} <b>RSSB Official Notification</b>\n\n`
            + `📋 <b>Category:</b> ${escapeHtml(cat.label)}\n`
            + `📅 <b>Published:</b> ${epochToIST(epochId)}\n\n`
            + `${cat.hashtag}`;
        for (const cid of targets) {
            enqueueDispatch(async () => {
                await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        chat_id: String(cid),
                        text: caption,
                        parse_mode: 'HTML',
                        reply_markup: {
                            inline_keyboard: [[{ text: '📥 Open Official Document', url: pdfUrl }]]
                        }
                    }),
                    signal: AbortSignal.timeout(15000)
                }).catch(() => {});
            });
        }
    }
    console.log(`======================================================\n`);
}

// ── Multi-Category Instant Loop (Runs EVERY SECOND) ───────────────────────
async function runScannerLoop() {
    console.log(`\n=====================================================`);
    console.log(`  RSSB High-Speed Multi-Category Scanner Started`);
    console.log(`  Monitored Categories: ${CATEGORIES.length} sections`);
    console.log(`  Watermark: ${WATERMARK_TEXT ? `ENABLED ("${WATERMARK_TEXT}")` : 'DISABLED'}`);
    console.log(`  Bot Token: ${BOT_TOKEN ? 'CONFIGURED (' + BOT_TOKEN.slice(0, 8) + '...)' : 'MISSING'}`);
    console.log(`  Bound Chats: ${boundChats.size ? Array.from(boundChats).join(', ') : 'NONE (Use /bind in Telegram)'}`);
    console.log(`  Frequency: Every 1 Second Parallel Probe`);
    console.log(`=====================================================\n`);

    let lastCatchup = nowEpoch();

    while (true) {
        const loopStart = Date.now();
        const now = nowEpoch();

        // 1. Probe all categories for current second in parallel
        const currentChecks = CATEGORIES.map(async (cat) => {
            const itemKey = `${cat.key}:${now}`;
            if (recentChecked.has(itemKey)) return;
            recentChecked.set(itemKey, now);
            stats.totalChecks++;

            const res = await checkCategoryPdf(cat.key, now);
            if (res.hit) {
                await onHitDetected(cat, now, res.size);
            }
        });
        await Promise.all(currentChecks);

        // 2. Every 10 seconds: sweep any missed past second (in gentle batches)
        if (now - lastCatchup >= 10) {
            lastCatchup = now;
            const past10 = Array.from({ length: 10 }, (_, i) => now - i - 1);

            for (const epochId of past10) {
                const batch = [];
                for (const cat of CATEGORIES) {
                    const itemKey = `${cat.key}:${epochId}`;
                    if (!recentChecked.has(itemKey)) {
                        recentChecked.set(itemKey, now);
                        stats.totalChecks++;
                        batch.push(
                            checkCategoryPdf(cat.key, epochId).then(async (res) => {
                                if (res.hit) {
                                    await onHitDetected(cat, epochId, res.size);
                                }
                            })
                        );
                    }
                }
                if (batch.length > 0) {
                    await Promise.all(batch);
                }
            }

            // Memory maintenance: prune rolling cache to keep only last 180 seconds
            const cutoffTime = now - 180;
            for (const [key, ts] of recentChecked.entries()) {
                if (ts < cutoffTime) recentChecked.delete(key);
            }
        }

        process.stdout.write(`\r[${epochToIST(now)}] Checks: ${stats.totalChecks} | Hits: ${stats.totalHits} | Chats: ${boundChats.size} `);

        const elapsed = Date.now() - loopStart;
        await new Promise(r => setTimeout(r, Math.max(0, 1000 - elapsed)));
    }
}

// ── Interactive Telegram Commands & Channel Support ───────────────────────
async function sendTelegramReply(chatId, text, replyMarkup = null) {
    if (!BOT_TOKEN || !chatId) return;
    try {
        const payload = {
            chat_id: String(chatId),
            text,
            parse_mode: 'HTML',
            disable_web_page_preview: true
        };
        if (replyMarkup) payload.reply_markup = replyMarkup;

        const res = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
            signal: AbortSignal.timeout(15000)
        });
        const data = await res.json();
        if (!data.ok) {
            console.error(`[TELEGRAM REPLY FAILED] ${chatId}: ${data.description}`);
        }
    } catch (e) {
        console.error('sendTelegramReply error:', e.message);
    }
}

async function handleCommand(msg) {
    const chatId = msg.chat.id;
    const chatType = msg.chat.type; // 'private', 'group', 'supergroup', 'channel'
    const text = (msg.text || '').trim();
    const parts = text.split(/\s+/);
    const cmd = parts[0].toLowerCase();
    const arg = parts[1];

    // Check if user forwarded a message from their channel to the bot in PM
    const fwdChat = msg.forward_from_chat || (msg.forward_origin && msg.forward_origin.chat);
    if (fwdChat && (fwdChat.type === 'channel' || fwdChat.type === 'supergroup')) {
        const fwdId = String(fwdChat.id);
        boundChats.add(fwdId);
        console.log(`[CHANNEL BOUND VIA FORWARD] Title: "${fwdChat.title}", ID: ${fwdId}`);
        await sendTelegramReply(chatId, `📢 <b>Channel Detected via Forward!</b>\n\n`
            + `• <b>Title:</b> ${escapeHtml(fwdChat.title || 'Channel')}\n`
            + `• <b>Channel ID:</b> <code>${fwdId}</code>\n`
            + (fwdChat.username ? `• <b>Username:</b> @${fwdChat.username}\n` : '')
            + `\n✅ <b>Channel bound successfully!</b> Live RSSB notices and PDFs will now be automatically dispatched here.\n\n`
            + `<i>💡 Tip: To make this permanent across Koyeb restarts, add <code>CHAT_ID=${fwdId}</code> to your environment variables.</i>`);
        return;
    }

    if (cmd === '/start' || cmd.startsWith('/start@')) {
        // If sent in a channel, confirm binding immediately!
        if (chatType === 'channel' || chatType === 'supergroup') {
            boundChats.add(String(chatId));
            const welcomeChannel = `🤖 <b>RSSB Instant Radar Bot Connected!</b>\n\n`
                + `📢 <b>Channel:</b> ${escapeHtml(msg.chat.title || 'Channel')}\n`
                + `🆔 <b>Channel ID:</b> <code>${chatId}</code>\n\n`
                + `✅ <b>This channel is now bound to receive instant RSSB updates!</b>\n`
                + `Every newly published RSSB result, answer key, admit card, or notice will be posted here within 1 second of arrival.`;
            await sendTelegramReply(chatId, welcomeChannel);
            return;
        }

        const welcome = `🤖 <b>RSSB Instant Radar Bot</b>\n\n`
            + `Monitoring Rajasthan Staff Selection Board (RSSB) every second for newly released notices and PDFs!\n\n`
            + `<b>Available Commands:</b>\n`
            + `• /status - Live scanner stats & uptime\n`
            + `• /check <code>&lt;id&gt;</code> - Manually probe any past ID (e.g. <code>/check 1793274315</code>)\n`
            + `• /testsend - Test sending sample notice & PDF\n`
            + `• /bind - Bind this chat or a channel to live alerts\n`
            + `• /id - Show chat/channel ID details\n`
            + `• /categories - Monitored RSSB categories\n`
            + `• /ping - Latency check`;
        await sendTelegramReply(chatId, welcome);
        return;
    }

    if (cmd === '/id' || cmd.startsWith('/id@')) {
        const title = msg.chat.title || `${msg.chat.first_name || ''} ${msg.chat.last_name || ''}`.trim() || 'Chat';
        const msgText = `🆔 <b>Chat Identification Details</b>\n\n`
            + `• <b>Title / Name:</b> ${escapeHtml(title)}\n`
            + `• <b>Chat ID:</b> <code>${chatId}</code>\n`
            + `• <b>Type:</b> <code>${chatType}</code>\n`
            + (msg.chat.username ? `• <b>Username:</b> @${msg.chat.username}\n` : '')
            + `• <b>Bound to Live Alerts:</b> ${boundChats.has(String(chatId)) ? '✅ YES' : '❌ NO (use /bind)'}\n\n`
            + `<i>To keep this destination permanent on Koyeb, set:</i>\n`
            + `<code>CHAT_ID=${chatId}</code>`;
        await sendTelegramReply(chatId, msgText);
        return;
    }

    if (cmd === '/bind' || cmd.startsWith('/bind@')) {
        // If run inside a channel or group directly
        if (chatType === 'channel' || chatType === 'supergroup') {
            boundChats.add(String(chatId));
            await sendTelegramReply(chatId, `✅ <b>Channel Successfully Bound!</b>\n\n`
                + `📢 <b>Channel:</b> ${escapeHtml(msg.chat.title)}\n`
                + `🆔 <b>Channel ID:</b> <code>${chatId}</code>\n\n`
                + `All future RSSB releases will be automatically dispatched here in real time.`);
            return;
        }

        // If run with an argument e.g. /bind @channel or /bind -10012345678
        if (arg) {
            const target = arg.trim();
            await sendTelegramReply(chatId, `⏳ Verifying channel <code>${escapeHtml(target)}</code>...`);
            try {
                const res = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getChat?chat_id=${encodeURIComponent(target)}`, {
                    signal: AbortSignal.timeout(15000)
                });
                const data = await res.json();
                if (!data.ok) {
                    await sendTelegramReply(chatId, `❌ <b>Could not access channel:</b> ${escapeHtml(data.description)}\n\n`
                        + `<b>Make sure:</b>\n`
                        + `1. The bot is added as an <b>Administrator</b> to the channel.\n`
                        + `2. The bot has permission to <b>Post Messages</b>.\n`
                        + `3. You typed the correct <code>@channel_username</code> or <code>-100...</code> ID.`);
                    return;
                }
                const targetChat = data.result;
                const targetId = String(targetChat.id);
                boundChats.add(targetId);

                // Send a test hello message to the channel to verify posting
                await sendTelegramReply(targetId, `🤖 <b>RSSB Instant Radar Bot Connected!</b>\n\n`
                    + `✅ This channel is now bound to receive instant RSSB updates with PDFs.`);

                await sendTelegramReply(chatId, `✅ <b>Success!</b> Bound channel <b>${escapeHtml(targetChat.title)}</b> (<code>${targetId}</code>).\n`
                    + `A confirmation message was posted to the channel.`);
            } catch (e) {
                await sendTelegramReply(chatId, `❌ Verification failed: ${escapeHtml(e.message)}`);
            }
            return;
        }

        // If run in PM without arguments, show current bound chats & guide
        const list = Array.from(boundChats);
        let msgText = `📢 <b>Channel Binding Manager</b>\n\n`
            + `<b>Active Destinations (${list.length}):</b>\n`;
        if (list.length === 0) {
            msgText += `<i>No channels bound yet!</i>\n`;
        } else {
            list.forEach((id, idx) => {
                msgText += `${idx + 1}. <code>${id}</code>\n`;
            });
        }

        msgText += `\n<b>How to bind your Channel:</b>\n`
            + `<b>Method 1 (Easiest):</b> Simply forward ANY post from your channel to this bot chat!\n`
            + `<b>Method 2:</b> Add this bot as an <b>Admin</b> in your channel, then send <code>/bind</code> in the channel.\n`
            + `<b>Method 3:</b> Type <code>/bind @your_channel_username</code> or <code>/bind -100xxxxxxxxxx</code> here.\n\n`
            + `<i>To bind this personal chat, send: <code>/bind ${chatId}</code></i>`;
        await sendTelegramReply(chatId, msgText);
        return;
    }

    if (cmd === '/status' || cmd.startsWith('/status@')) {
        const uptime = fmtUptime(Date.now() - stats.startedAt);
        const last = stats.lastHit ? `${stats.lastHit.cat} (${stats.lastHit.date})` : 'None yet';
        const msgText = `📊 <b>RSSB Scanner Status</b>\n\n`
            + `⏱ <b>Uptime:</b> ${uptime}\n`
            + `🔍 <b>Total Checks:</b> ${stats.totalChecks.toLocaleString()}\n`
            + `⚡ <b>Speed:</b> ~${CATEGORIES.length} checks/second\n`
            + `🏆 <b>Total Hits:</b> ${stats.totalHits}\n`
            + `📤 <b>Sent to Telegram:</b> ${stats.sentCount}\n`
            + `📢 <b>Bound Destinations:</b> ${boundChats.size} chat(s)\n`
            + `🎨 <b>Watermarking:</b> ${WATERMARK_TEXT ? `Active ("${escapeHtml(WATERMARK_TEXT)}")` : 'Disabled'}\n`
            + `🎯 <b>Monitored Folders:</b> ${CATEGORIES.length}\n`
            + `🕒 <b>Last Discovery:</b> ${last}`;
        await sendTelegramReply(chatId, msgText);
        return;
    }

    if (cmd === '/categories' || cmd.startsWith('/categories@')) {
        let msgText = `📋 <b>Monitored Categories (Every 1s):</b>\n\n`;
        CATEGORIES.forEach((c, idx) => {
            msgText += `${idx + 1}. ${c.icon} <b>${escapeHtml(c.label)}</b>\n   <code>/storage/${c.key}/</code>\n`;
        });
        await sendTelegramReply(chatId, msgText);
        return;
    }

    if (cmd === '/check' || cmd.startsWith('/check@')) {
        if (!arg) {
            await sendTelegramReply(chatId, `⚠ Usage: <code>/check 1793274315</code>`);
            return;
        }
        const cleanId = arg.replace(/\D/g, '');
        if (!cleanId) {
            await sendTelegramReply(chatId, `⚠ Please provide a valid numeric timestamp ID (e.g. <code>/check 1793274315</code>).`);
            return;
        }

        await sendTelegramReply(chatId, `🔍 Probing ID <code>${cleanId}</code> across all ${CATEGORIES.length} RSSB categories...`);

        // Probe all categories with dedicated manualAgent and generous 15s timeout
        const results = await Promise.all(CATEGORIES.map(cat => probeManualCategoryPdf(cat.key, cleanId, 15000)));
        const hits = results.filter(r => r.hit);

        if (hits.length === 0) {
            const timedOut = results.filter(r => r.status === 0 || r.error);
            if (timedOut.length === CATEGORIES.length) {
                await sendTelegramReply(chatId, `⚠ <b>Network Timeout:</b> Could not reach RSSB server (all requests timed out). RSSB server might be temporarily slow. Please try running <code>/check ${cleanId}</code> again.`);
            } else {
                await sendTelegramReply(chatId, `❌ No files found for ID <code>${cleanId}</code> across any category (HTTP 404).`);
            }
        } else {
            for (const h of hits) {
                const cat = CATEGORIES.find(c => c.key === h.catKey);
                await sendTelegramReply(chatId, `🎯 Found in <b>${escapeHtml(cat.label)}</b> (${Math.round(h.size / 1024)} KB)!\n📥 Downloading & dispatching notice...`);
                try {
                    const buf = await downloadPdf(h.catKey, cleanId);
                    const sendRes = await sendPdfToChat(chatId, cat, cleanId, buf);
                    if (sendRes && sendRes.success === false) {
                        await sendTelegramReply(chatId, `⚠ Notice card sent, but document upload failed: ${escapeHtml(sendRes.error)}`);
                    }
                } catch (e) {
                    await sendTelegramReply(chatId, `⚠ Download failed: ${escapeHtml(e.message)}\n🔗 Direct URL: ${RSSB_BASE}/storage/${h.catKey}/${cleanId}.pdf`);
                }
            }
        }
        return;
    }

    if (cmd === '/testsend' || cmd.startsWith('/testsend@')) {
        await sendTelegramReply(chatId, `⏳ Downloading sample PDF & applying branding... Please wait a few seconds.`);
        const sampleCat = CATEGORIES[0]; // result_item
        const sampleId = '1793125170';
        try {
            const buf = await downloadPdf(sampleCat.key, sampleId);
            const sendRes = await sendPdfToChat(chatId, sampleCat, sampleId, buf);
            if (sendRes && sendRes.success === false) {
                await sendTelegramReply(chatId, `⚠ Notice link sent, but Telegram rejected PDF upload: ${escapeHtml(sendRes.error)}`);
            } else {
                await sendTelegramReply(chatId, `✅ <b>Test Notice & PDF successfully delivered!</b>`);
            }
        } catch (e) {
            await sendTelegramReply(chatId, `❌ Test download or processing failed: ${escapeHtml(e.message)}`);
        }
        return;
    }

    if (cmd === '/ping' || cmd.startsWith('/ping@')) {
        const start = Date.now();
        await sendTelegramReply(chatId, `🏓 Pong! Latency: ${Date.now() - start}ms`);
        return;
    }

    if (cmd === '/help' || cmd.startsWith('/help@')) {
        await sendTelegramReply(chatId, `<b>RSSB Radar Bot Help:</b>\n\n`
            + `• /status - Scanner health & stats\n`
            + `• /check &lt;id&gt; - Probe any timestamp ID (e.g. <code>/check 1793274315</code>)\n`
            + `• /testsend - Test sending sample notice & PDF\n`
            + `• /bind - Bind channel or group to alerts\n`
            + `• /id - Show chat/channel ID details\n`
            + `• /categories - Monitored categories\n`
            + `• /ping - Ping check`);
    }
}

// ── Telegram Polling Loop (Handles Messages, Channel Posts & Admin Events) ─
async function startTelegramPoller() {
    if (!BOT_TOKEN) {
        console.warn('[TELEGRAM] BOT_TOKEN is empty. Polling disabled.');
        return;
    }
    let offset = 0;
    console.log('[TELEGRAM] Command poller started (listening for PMs, Channel Posts & Events)...');

    while (true) {
        try {
            const res = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getUpdates?offset=${offset}&timeout=25`, {
                signal: AbortSignal.timeout(35000)
            });
            const data = await res.json();
            if (data.ok && Array.isArray(data.result)) {
                for (const update of data.result) {
                    offset = update.update_id + 1;

                    // 1. Handle messages (PMs & Groups) or channel posts
                    const msg = update.message || update.channel_post;
                    if (msg) {
                        handleCommand(msg).catch((err) => console.error('handleCommand error:', err.message));
                    }

                    // 2. Handle bot added as Administrator to channel
                    if (update.my_chat_member) {
                        const chat = update.my_chat_member.chat;
                        const status = update.my_chat_member.new_chat_member?.status;
                        if (status === 'administrator' && (chat.type === 'channel' || chat.type === 'supergroup')) {
                            const cid = String(chat.id);
                            boundChats.add(cid);
                            console.log(`[CHANNEL CONNECTED] Bot added as admin to "${chat.title}" (${cid})`);
                            sendTelegramReply(cid, `🤖 <b>RSSB Instant Radar Bot Connected!</b>\n\n`
                                + `📢 <b>Channel:</b> ${escapeHtml(chat.title)}\n`
                                + `🆔 <b>Channel ID:</b> <code>${cid}</code>\n\n`
                                + `✅ <b>This channel is now bound to receive instant RSSB updates!</b>\n`
                                + `New notifications and PDFs will be dispatched here within 1 second of publication.`).catch(() => {});
                        }
                    }
                }
            }
        } catch (e) {
            await new Promise(r => setTimeout(r, 3000));
        }
    }
}

// ── Built-in HTTP Health Check Server (Required for Koyeb) ─────────────────
const healthServer = http.createServer((req, res) => {
    if (req.url === '/health' || req.url === '/') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({
            status: 'healthy',
            uptime: fmtUptime(Date.now() - stats.startedAt),
            totalChecks: stats.totalChecks,
            hits: stats.totalHits,
            sentCount: stats.sentCount,
            boundChats: boundChats.size,
            categories: CATEGORIES.length,
            watermark: WATERMARK_TEXT ? 'active' : 'disabled'
        }));
    }
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not Found');
});

// ── Process Crash Protections ─────────────────────────────────────────────
process.on('uncaughtException', (err) => {
    console.error('[CRASH GUARD: UNCAUGHT EXCEPTION]', err.message);
});
process.on('unhandledRejection', (reason) => {
    console.error('[CRASH GUARD: UNHANDLED REJECTION]', reason);
});

// Start Service
healthServer.listen(PORT, () => {
    console.log(`Health check server listening on :${PORT}`);
    runScannerLoop();
    startTelegramPoller();
});
