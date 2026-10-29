/**
 * RSSB Instant Multi-Category Telegram Bot with PDF Watermarking
 * -------------------------------------------------------------
 * • 1-Second Multi-Category Prober across 8 RSSB storage folders
 * • Dedicated separate download pipeline (No socket starvation or premature timeouts)
 * • Automatic PDF Watermarking & Clickable Banners via pdf-lib
 * • 50 MB Telegram upload safeguard (auto-switches to link if > 49MB)
 * • Inline Action Buttons (Direct Link + 1-Tap Share to Friends)
 * • Sequential dispatch queue to avoid Telegram 429 Rate Limits
 * • Memory-bounded rolling window (Zero memory leak on months-long runs)
 * • Global crash guard (uncaughtException & unhandledRejection handlers)
 * • Interactive Admin/User commands (/status, /check, /categories, /ping, /testsend)
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
    // If running without npm install, works smoothly without failing
}

// ── ENVIRONMENT VARIABLES ─────────────────────────────────────────────────
const PORT = process.env.PORT || 8000;
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const CHAT_IDS = process.env.CHAT_IDS
    ? process.env.CHAT_IDS.split(',').map(s => s.trim()).filter(Boolean)
    : (CHAT_ID ? [CHAT_ID] : []);

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

// Filter out excluded categories (e.g. tenders)
const CATEGORIES = ALL_CATEGORIES.filter(c => !EXCLUDE_CATEGORIES.includes(c.key));

const RSSB_BASE = 'https://rssb.rajasthan.gov.in';

// Dedicated HTTPS Agent for 1-second HEAD scanning checks
const scanAgent = new https.Agent({
    secureOptions: crypto.constants.SSL_OP_LEGACY_SERVER_CONNECT || 0x4,
    rejectUnauthorized: false,
    keepAlive: true,
    maxSockets: 150,
    maxFreeSockets: 80
});

// Dedicated HTTPS Agent for downloading full PDF files (never starved by HEAD checks)
const downloadAgent = new https.Agent({
    secureOptions: crypto.constants.SSL_OP_LEGACY_SERVER_CONNECT || 0x4,
    rejectUnauthorized: false,
    keepAlive: true,
    maxSockets: 20,
    maxFreeSockets: 10
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
    'result_item:1793125170', 'result_item:1793125287', 'result_item:1793125321',
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

// ── Ultra-Fast Single Category HEAD Probe ─────────────────────────────────
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
            timeout: 4500
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

// Wrapper with 1 immediate retry if network drops
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
        const tail = buffer.subarray(Math.max(0, buffer.length - 8192)).toString('latin1');
        const m = tail.match(/\/Title\s*\(([^)]+)\)/);
        if (m && m[1]) return m[1].trim();
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

        const watermarkText = WATERMARK_TEXT;
        const bannerText = BANNER_TEXT || WATERMARK_TEXT;

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
    if (!BOT_TOKEN || !chatId) return;

    const docTitle = extractPdfTitle(rawBuffer);
    const sizeBytes = rawBuffer.length;
    const sizeKB = Math.round(sizeBytes / 1024);
    const sizeMB = (sizeBytes / (1024 * 1024)).toFixed(1);
    const pdfUrl = `${RSSB_BASE}/storage/${cat.key}/${epochId}.pdf`;
    const dateStr = epochToIST(epochId);

    // 1. Clean caption (NO raw epoch ID shown)
    let caption = `${cat.icon} <b>RSSB Official Notification</b>\n\n`
                + `📋 <b>Category:</b> ${cat.label}\n`;

    if (docTitle) {
        caption += `📄 <b>Notice:</b> <code>${docTitle}</code>\n`;
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
        await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                chat_id: String(chatId),
                text: caption + `\n\n⚠ <i>Notice file is ${sizeMB}MB (exceeds Telegram 50MB limit). Download directly via the button below:</i>`,
                parse_mode: 'HTML',
                reply_markup: replyMarkup
            })
        });
        stats.sentCount++;
        return;
    }

    // 4. Apply Watermarking
    const { brandedBytes, modified } = await brandPdf(rawBuffer);
    const finalBytes = brandedBytes;
    const filename = docTitle ? docTitle.replace(/[\\/:*?"<>|]/g, '_') : `RSSB_${cat.label.replace(/\s+/g, '_')}.pdf`;

    try {
        const form = new FormData();
        form.append('chat_id', String(chatId));
        form.append('caption', caption);
        form.append('parse_mode', 'HTML');
        form.append('reply_markup', JSON.stringify(replyMarkup));
        form.append('document', new Blob([finalBytes], { type: 'application/pdf' }), filename);

        const res = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
            method: 'POST',
            body: form
        });
        const data = await res.json();
        if (!data.ok) throw new Error(data.description);

        stats.sentCount++;
        console.log(`[TELEGRAM] ${modified ? 'Branded' : 'Original'} "${filename}" sent to ${chatId}`);
    } catch (err) {
        console.error(`[TELEGRAM ERROR] ${chatId}: ${err.message}`);
        // Fallback to text card with direct link
        try {
            await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    chat_id: String(chatId),
                    text: caption,
                    parse_mode: 'HTML',
                    reply_markup: replyMarkup
                })
            });
        } catch {}
    }
}

function broadcastHit(cat, epochId, pdfBuffer) {
    const targets = new Set(CHAT_IDS.filter(Boolean));
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
        const targets = new Set(CHAT_IDS.filter(Boolean));
        const pdfUrl = `${RSSB_BASE}/storage/${cat.key}/${epochId}.pdf`;
        const caption = `${cat.icon} <b>RSSB Official Notification</b>\n\n`
            + `📋 <b>Category:</b> ${cat.label}\n`
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
                    })
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
    console.log(`  Target Chats: ${CHAT_IDS.length ? CHAT_IDS.join(', ') : 'NONE'}`);
    console.log(`  Frequency: Every 1 Second + 10s Catchup Sweep`);
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

        // 2. Every 10 seconds: sweep past 10 seconds across all categories
        if (now - lastCatchup >= 10) {
            lastCatchup = now;
            const past10 = Array.from({ length: 10 }, (_, i) => now - i - 1);
            const catchupChecks = [];

            for (const epochId of past10) {
                for (const cat of CATEGORIES) {
                    const itemKey = `${cat.key}:${epochId}`;
                    if (!recentChecked.has(itemKey)) {
                        recentChecked.set(itemKey, now);
                        stats.totalChecks++;
                        catchupChecks.push(
                            checkCategoryPdf(cat.key, epochId).then(async (res) => {
                                if (res.hit) {
                                    await onHitDetected(cat, epochId, res.size);
                                }
                            })
                        );
                    }
                }
            }
            if (catchupChecks.length > 0) {
                await Promise.all(catchupChecks);
            }

            // Memory maintenance: prune rolling cache to keep only last 120 seconds
            const cutoffTime = now - 120;
            for (const [key, ts] of recentChecked.entries()) {
                if (ts < cutoffTime) recentChecked.delete(key);
            }
        }

        process.stdout.write(`\r[${epochToIST(now)}] Checks: ${stats.totalChecks} | Hits: ${stats.totalHits} `);

        const elapsed = Date.now() - loopStart;
        await new Promise(r => setTimeout(r, Math.max(0, 1000 - elapsed)));
    }
}

// ── Interactive Telegram Commands ─────────────────────────────────────────
async function sendTelegramReply(chatId, text, replyMarkup = null) {
    if (!BOT_TOKEN) return;
    try {
        const payload = {
            chat_id: String(chatId),
            text,
            parse_mode: 'HTML',
            disable_web_page_preview: true
        };
        if (replyMarkup) payload.reply_markup = replyMarkup;

        await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
    } catch (e) {
        console.error('sendTelegramReply error:', e.message);
    }
}

async function handleCommand(msg) {
    const chatId = msg.chat.id;
    const text = (msg.text || '').trim();
    const parts = text.split(/\s+/);
    const cmd = parts[0].toLowerCase();
    const arg = parts[1];

    if (cmd === '/start' || cmd.startsWith('/start@')) {
        const welcome = `🤖 <b>RSSB Instant Radar Bot</b>\n\n`
            + `Monitoring Rajasthan Staff Selection Board (RSSB) every second for newly released notices and PDFs!\n\n`
            + `<b>Available Commands:</b>\n`
            + `• /status - Live scanner stats & uptime\n`
            + `• /categories - Monitored RSSB categories\n`
            + `• /check <code>&lt;id&gt;</code> - Manually probe any past ID\n`
            + `• /testsend - Test sending a sample watermarked PDF\n`
            + `• /ping - Latency check`;
        await sendTelegramReply(chatId, welcome);
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
            + `🎨 <b>Watermarking:</b> ${WATERMARK_TEXT ? `Active ("${WATERMARK_TEXT}")` : 'Disabled'}\n`
            + `🎯 <b>Monitored Folders:</b> ${CATEGORIES.length}\n`
            + `🕒 <b>Last Discovery:</b> ${last}`;
        await sendTelegramReply(chatId, msgText);
        return;
    }

    if (cmd === '/categories' || cmd.startsWith('/categories@')) {
        let msgText = `📋 <b>Monitored Categories (Every 1s):</b>\n\n`;
        CATEGORIES.forEach((c, idx) => {
            msgText += `${idx + 1}. ${c.icon} <b>${c.label}</b>\n   <code>/storage/${c.key}/</code>\n`;
        });
        await sendTelegramReply(chatId, msgText);
        return;
    }

    if (cmd === '/check' || cmd.startsWith('/check@')) {
        if (!arg) {
            await sendTelegramReply(chatId, `⚠ Usage: <code>/check 1793125170</code>`);
            return;
        }
        const cleanId = arg.replace(/\D/g, '');
        if (!cleanId) {
            await sendTelegramReply(chatId, `⚠ Please provide a valid numeric ID.`);
            return;
        }

        await sendTelegramReply(chatId, `🔍 Searching ID <code>${cleanId}</code> across all categories...`);
        const results = await Promise.all(CATEGORIES.map(cat => checkCategoryPdf(cat.key, cleanId)));
        const hits = results.filter(r => r.hit);

        if (hits.length === 0) {
            await sendTelegramReply(chatId, `❌ No files found for ID <code>${cleanId}</code> across any category.`);
        } else {
            for (const h of hits) {
                const cat = CATEGORIES.find(c => c.key === h.catKey);
                try {
                    const buf = await downloadPdf(h.catKey, cleanId);
                    await sendPdfToChat(chatId, cat, cleanId, buf);
                } catch (e) {
                    await sendTelegramReply(chatId, `Found in ${cat.label} (${Math.round(h.size / 1024)}KB) but download failed: ${e.message}`);
                }
            }
        }
        return;
    }

    if (cmd === '/testsend' || cmd.startsWith('/testsend@')) {
        await sendTelegramReply(chatId, `⏳ Downloading sample PDF & applying branding... Please wait a few seconds.`);
        const sampleCat = CATEGORIES[0];
        const sampleId = '1793125170';
        try {
            const buf = await downloadPdf(sampleCat.key, sampleId);
            await sendPdfToChat(chatId, sampleCat, sampleId, buf);
            await sendTelegramReply(chatId, `✅ Test notice & PDF successfully dispatched!`);
        } catch (e) {
            await sendTelegramReply(chatId, `❌ Test download failed: ${e.message}`);
        }
        return;
    }

    if (cmd === '/ping' || cmd.startsWith('/ping@')) {
        const start = Date.now();
        await sendTelegramReply(chatId, `🏓 Pong! Latency: ${Date.now() - start}ms`);
        return;
    }

    if (cmd === '/help' || cmd.startsWith('/help@')) {
        await sendTelegramReply(chatId, `<b>Bot Help:</b>\n\n`
            + `/status - Speed & health metrics\n`
            + `/categories - Active categories\n`
            + `/check &lt;id&gt; - Probe any timestamp ID\n`
            + `/testsend - Test sample watermarked PDF\n`
            + `/ping - Health check`);
    }
}

// Telegram polling loop
async function startTelegramPoller() {
    if (!BOT_TOKEN) return;
    let offset = 0;
    while (true) {
        try {
            const res = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getUpdates?offset=${offset}&timeout=25`);
            const data = await res.json();
            if (data.ok && Array.isArray(data.result)) {
                for (const update of data.result) {
                    offset = update.update_id + 1;
                    if (update.message && update.message.text) {
                        handleCommand(update.message).catch(() => {});
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
