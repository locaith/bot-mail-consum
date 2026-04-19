require('dotenv').config();

const axios = require('axios');
const fs = require('fs');
const path = require('path');
const chalk = require('chalk');
const readline = require('readline');
const ExcelJS = require('exceljs');
const { DateTime } = require('luxon');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const { chromium } = require('playwright');

const ROOT_DIR = __dirname;
const ACCOUNTS_FILE = path.join(ROOT_DIR, 'accounts.json');
const SELECTED_ACCOUNTS_FILE = path.join(ROOT_DIR, 'selected_accounts.txt');
const OUTPUT_EXCEL = path.resolve(ROOT_DIR, process.env.OUTPUT_EXCEL || './output/gmail_ai_output.xlsx');
const MAX_ACCOUNT_THREADS = parseInt(process.env.MAX_ACCOUNT_THREADS || '3', 10);
const DEFAULT_MAX_EMAILS = parseInt(process.env.DEFAULT_MAX_EMAILS || '50', 10);
const DEFAULT_MAIL_CONCURRENCY = parseInt(process.env.DEFAULT_MAIL_CONCURRENCY || '10', 10);
const TIMEZONE = process.env.TIMEZONE || 'Asia/Ho_Chi_Minh';
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || '';
const GEMINI_MODEL = process.env.GEMINI_MODEL || 'gemini-3-flash-preview';
const INTRO_NAME = process.env.INTRO_NAME || 'dev Ha';

const EXCEL_HEADERS = [
  'account_id', 'email', 'message_key', 'gmail_url', 'thread_url', 'sender_name', 'sender_email', 'subject',
  'received_at_raw', 'received_at_iso', 'received_at_local', 'body_text', 'ai_json', 'ai_status',
  'ai_confidence', 'ai_error', 'request_type', 'customer_name', 'phone', 'email_extracted', 'order_code',
  'amount', 'currency', 'note', 'created_at'
];

// --- UTILS ---
function ensureDir(dirPath) {
  if (!fs.existsSync(dirPath)) fs.mkdirSync(dirPath, { recursive: true });
}

function fileExists(filePath) {
  try { fs.accessSync(filePath, fs.constants.F_OK); return true; } catch (_) { return false; }
}

function safeJsonRead(filePath, fallback) {
  try { if (!fileExists(filePath)) return fallback; return JSON.parse(fs.readFileSync(filePath, 'utf8')); } catch (_) { return fallback; }
}

function safeJsonWrite(filePath, data) {
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8');
}

function cleanText(value) {
  return String(value || '').replace(/\u200b/g, ' ').replace(/\s+/g, ' ').trim();
}

function escapeFormula(value) {
  const text = String(value == null ? '' : value);
  return /^[=+\-@]/.test(text) ? `'${text}` : text;
}

function chunkArray(items, size) {
  const chunks = [];
  for (let i = 0; i < items.length; i += size) chunks.push(items.slice(i, i + size));
  return chunks;
}

function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

function ask(question) {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise(resolve => {
    rl.question(question, answer => { rl.close(); resolve(answer); });
  });
}

function banner() {
  console.log(chalk.magenta('============================================================'));
  console.log(chalk.cyan.bold(`      GMAIL WEB GEMINI BOT - Script by ${INTRO_NAME}`));
  console.log(chalk.magenta('============================================================'));
}

function loadAccounts() {
  if (!fileExists(ACCOUNTS_FILE)) throw new Error(`Không tìm thấy ${ACCOUNTS_FILE}.`);
  const accounts = safeJsonRead(ACCOUNTS_FILE, []);
  return accounts.map(account => ({
    ...account,
    maxEmails: Number(account.maxEmails || DEFAULT_MAX_EMAILS),
    mailConcurrency: Number(account.mailConcurrency || DEFAULT_MAIL_CONCURRENCY),
    profileDir: account.profileDir || `./profiles/${account.id}`,
    gmailUrl: account.gmailUrl || 'https://mail.google.com/mail/u/0/#inbox',
    enabled: account.enabled !== false,
    onlyUnread: !!account.onlyUnread,
    query: account.query || '',
    proxy: account.proxy || ''
  }));
}

function loadSelectedAccounts(allAccounts) {
  if (!fileExists(SELECTED_ACCOUNTS_FILE)) return allAccounts.filter(acc => acc.enabled);
  const selectedIds = fs.readFileSync(SELECTED_ACCOUNTS_FILE, 'utf8').split('\n').map(item => item.trim()).filter(Boolean);
  if (selectedIds.length === 0) return allAccounts.filter(acc => acc.enabled);
  return allAccounts.filter(acc => acc.enabled && selectedIds.includes(acc.id));
}

// --- EXCEL WRITER (UPSERT MODE) ---
class ExcelWriter {
  constructor(outputFile) {
    this.outputFile = outputFile;
    this.queue = Promise.resolve();
    ensureDir(path.dirname(this.outputFile));
  }

  async initWorkbook() {
    const workbook = new ExcelJS.Workbook();
    if (fileExists(this.outputFile)) await workbook.xlsx.readFile(this.outputFile);
    let sheet = workbook.getWorksheet('emails');
    if (!sheet) {
      sheet = workbook.addWorksheet('emails');
      sheet.addRow(EXCEL_HEADERS);
      sheet.getRow(1).font = { bold: true };
      sheet.columns = EXCEL_HEADERS.map(header => ({ header, key: header, width: 24 }));
    }
    return { workbook, sheet };
  }

  async writeMails(rows) {
    this.queue = this.queue.then(async () => {
      if (!rows || rows.length === 0) return;
      const { workbook, sheet } = await this.initWorkbook();

      for (const rowData of rows) {
        let existingRowNumber = -1;
        const threadUrl = rowData.thread_url;

        if (threadUrl) {
          sheet.eachRow((row, rowNumber) => {
            if (rowNumber === 1) return;
            // Cột 5 thường là thread_url (index 5)
            if (row.getCell(5).value === threadUrl) existingRowNumber = rowNumber;
          });
        }

        const excelRowValues = EXCEL_HEADERS.map(key => escapeFormula(rowData[key] ?? ''));

        if (existingRowNumber !== -1) {
          const row = sheet.getRow(existingRowNumber);
          excelRowValues.forEach((val, index) => { row.getCell(index + 1).value = val; });
          row.commit();
        } else {
          sheet.addRow(excelRowValues);
        }
      }
      await workbook.xlsx.writeFile(this.outputFile);
    });
    return this.queue;
  }
}

// --- SELF-HEALING AGENT ---
class SelfHealingAgent {
  constructor(geminiModel) { this.model = geminiModel; }
  async log(msg) { console.log(chalk.yellow(`[AUTO-AGENT] ${msg}`)); }
  async analyzeUI(htmlContent, errorType) {
    await this.log(`Phân tích UI Gmail (Lỗi: ${errorType})...`);
    const prompt = `Giao diện Gmail đã thay đổi. Phân tích HTML bên dưới và trả về JSON selector cho: { "inputSearch": "", "rowSelector": "", "threadIdAttr": "" }\nHTML: ${htmlContent.slice(0, 10000)}`;
    try {
      if (!GEMINI_API_KEY) return null;
      const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(GEMINI_MODEL)}:generateContent?key=${encodeURIComponent(GEMINI_API_KEY)}`;
      const res = await axios.post(url, { contents: [{ parts: [{ text: prompt }] }], generationConfig: { temperature: 0.1, responseMimeType: 'application/json' } });
      return JSON.parse(res.data.candidates[0].content.parts[0].text);
    } catch (_) { return null; }
  }
}

// --- GMAIL BOT ---
class GmailWebBot {
  constructor(account, accountIndex) {
    this.account = account;
    this.accountIndex = accountIndex;
    this.profileDir = path.resolve(ROOT_DIR, account.profileDir);
    this.stateFile = path.join(ROOT_DIR, 'state', `${account.id}_processed.json`);
    this.browserContext = null;
    this.page = null;
    this.agent = new SelfHealingAgent(GEMINI_MODEL);
    this.processed = safeJsonRead(this.stateFile, { keys: {}, updatedAt: null });
    ensureDir(this.profileDir);
    ensureDir(path.dirname(this.stateFile));
  }

  async log(msg, type = 'info') {
    const prefix = chalk.blue(`[Tài khoản ${this.accountIndex + 1} - ${this.account.email}]`);
    let output = `${prefix} ${msg}`;
    if (type === 'success') output = chalk.green(output);
    else if (type === 'error') output = chalk.red(output);
    else if (type === 'warning') output = chalk.yellow(output);
    console.log(output);
  }

  async randomDelay(min = 1000, max = 2500) { return sleep(Math.floor(Math.random() * (max - min + 1)) + min); }

  async launchContext() {
    const isHeadless = String(process.env.HEADLESS || 'true').toLowerCase() === 'true';
    const args = ['--disable-blink-features=AutomationControlled', '--start-maximized', '--lang=vi-VN'];
    if (this.account.proxy) args.push(`--proxy-server=${this.account.proxy}`);
    
    this.browserContext = await chromium.launchPersistentContext(this.profileDir, {
      headless: isHeadless, channel: 'chromium', viewport: null, locale: 'vi-VN', timezoneId: TIMEZONE, args
    });
    this.page = this.browserContext.pages()[0] || await this.browserContext.newPage();
    this.page.setDefaultTimeout(30000);
  }

  async closeContext() { try { if (this.browserContext) await this.browserContext.close(); } catch (_) {} }

  async dismissPopups() {
    const selectors = ['button[aria-label="Đóng"]', 'button[aria-label="No thanks"]', 'div[role="dialog"] button', '.th[role="alert"] .asb'];
    for (const s of selectors) {
      try { const btn = this.page.locator(s).first(); if (await btn.isVisible()) { await btn.click(); await sleep(500); } } catch (_) {}
    }
  }

  async gotoInbox() {
    await this.page.goto(this.account.gmailUrl, { waitUntil: 'domcontentloaded' });
    await this.randomDelay(3000, 5000);
    await this.dismissPopups();
  }

  async applySearchQuery() {
    if (!this.account.query) return;
    const selectors = ['input[placeholder*="Search"]', 'input[aria-label*="Search"]', 'input[name="q"]'];
    let searchBox = null;
    for (const s of selectors) {
      if (await this.page.locator(s).first().count() > 0) { searchBox = this.page.locator(s).first(); break; }
    }
    if (!searchBox) {
      const found = await this.agent.analyzeUI(await this.page.content(), 'Missing Search Box');
      if (found?.inputSearch) searchBox = this.page.locator(found.inputSearch).first();
    }
    if (searchBox) {
      await searchBox.click(); await searchBox.fill(''); await searchBox.type(this.account.query, { delay: 40 });
      await this.page.keyboard.press('Enter'); await this.randomDelay(2000, 3000);
    }
  }

  async scrapeBatch(onlyUnread) {
    return await this.page.evaluate(({ onlyUnread }) => {
      const threads = Array.from(document.querySelectorAll('[data-thread-id], [data-legacy-thread-id], .zA, tr[role="row"]'));
      const items = [];
      const seenIds = new Set();
      for (const el of threads) {
        const tid = el.getAttribute('data-thread-id') || el.getAttribute('data-legacy-thread-id');
        let row = el.closest('tr, div[role="row"], .zA');
        if (!row) continue;
        const link = row.querySelector('a[href*="/"]');
        let href = link ? link.getAttribute('href') : (tid ? `#inbox/${tid.replace('#thread-f:', '')}` : '');
        if (!href) continue;
        const uid = tid || href;
        if (seenIds.has(uid)) continue; seenIds.add(uid);
        const unread = row.classList.contains('zE') || !!row.querySelector('b');
        if (onlyUnread && !unread) continue;
        items.push({ href, preview: (row.innerText || '').replace(/\s+/g, ' ').slice(0, 100), unread });
      }
      return items;
    }, { onlyUnread });
  }

  async collectLatestEmailTargets() {
    const targetCount = this.account.maxEmails;
    const targets = [];
    const seenUrls = new Set();
    let noGrowth = 0;

    // Logic: Chỉ dừng khi THU THẬP ĐỦ số mail MỚI (chưa xử lý hoặc forceProcess=true)
    while (targets.length < targetCount && noGrowth < 6) {
      let batch = await this.scrapeBatch(this.account.onlyUnread);
      if (batch.length === 0) { await this.page.reload(); await sleep(5000); batch = await this.scrapeBatch(this.account.onlyUnread); }
      
      const beforeCount = targets.length;
      for (const item of batch) {
        const url = item.href.startsWith('http') ? item.href : `https://mail.google.com/mail/u/0/${item.href.replace(/^\//, '')}`;
        if (seenUrls.has(url)) continue;
        seenUrls.add(url);

        // Kiểm tra xem mail này đã từng được xử lý chưa (dựa trên URL/ThreadID trong state)
        const isOld = Object.values(this.processed.keys).some(k => k.url === url);
        if (!isOld || workerData.forceProcess) {
          targets.push({ ...item, url });
          if (targets.length >= targetCount) break;
        }
      }

      if (targets.length === beforeCount) {
        noGrowth++;
        await this.page.mouse.wheel(0, 3500);
        await this.randomDelay(1500, 2500);
      } else { noGrowth = 0; }
    }
    return targets;
  }

  async processSingleTarget(target) {
    const detail = await (async () => {
      const p = await this.browserContext.newPage();
      try {
        await p.goto(target.url); await sleep(3000);
        return await p.evaluate(() => ({
          subject: document.querySelector('h2.hP')?.textContent || document.querySelector('h2')?.textContent || '',
          senderEmail: document.querySelector('span[email]')?.getAttribute('email') || '',
          senderName: document.querySelector('span.gD')?.textContent || '',
          body: document.body.innerText || '',
          rawTimestamp: document.querySelector('span.g3')?.getAttribute('title') || new Date().toISOString(),
          pageUrl: location.href
        }));
      } finally { await p.close(); }
    })();

    const messageKey = `${this.account.id}|${detail.senderEmail}|${detail.subject}|${detail.rawTimestamp.slice(0, 50)}`.toLowerCase();
    
    // Gọi Gemini AI
    let ai = { ai_status: 'skipped' };
    if (GEMINI_API_KEY) {
      const prompt = `Trích xuất thông tin JSON: { "request_type": "", "customer_name": "", "phone": "", "email_extracted": "", "order_code": "", "amount": "", "currency": "", "note": "", "confidence": 0 }\nNội dung: ${detail.subject}\n${detail.body.slice(0, 2000)}`;
      try {
        const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(GEMINI_MODEL)}:generateContent?key=${encodeURIComponent(GEMINI_API_KEY)}`;
        const res = await axios.post(url, { contents: [{ parts: [{ text: prompt }] }], generationConfig: { temperature: 0.1, responseMimeType: 'application/json' } }, { timeout: 30000 });
        const parsed = JSON.parse(res.data.candidates[0].content.parts[0].text);
        ai = { ai_status: 'success', ai_json: JSON.stringify(parsed), ...parsed, ai_confidence: parsed.confidence || 0 };
      } catch (e) { ai = { ai_status: 'error', ai_error: e.message }; }
    }

    const row = {
      account_id: this.account.id, email: this.account.email, message_key: messageKey,
      gmail_url: this.account.gmailUrl, thread_url: detail.pageUrl,
      sender_name: detail.senderName, sender_email: detail.senderEmail, subject: detail.subject,
      received_at_raw: detail.rawTimestamp, body_text: detail.body.slice(0, 500),
      ai_status: ai.ai_status, ai_json: ai.ai_json, ai_confidence: ai.ai_confidence, ai_error: ai.ai_error,
      request_type: ai.request_type, customer_name: ai.customer_name, phone: ai.phone,
      order_code: ai.order_code, amount: ai.amount, currency: ai.currency, note: ai.note,
      created_at: new Date().toISOString()
    };

    this.processed.keys[messageKey] = { url: detail.pageUrl, updatedAt: row.created_at };
    safeJsonWrite(this.stateFile, this.processed);
    await this.log(`Đã xử lý: ${detail.subject}`, 'success');
    return row;
  }

  async runAccount() {
    try {
      await this.launchContext();
      await this.gotoInbox();
      await this.applySearchQuery();
      const targets = await this.collectLatestEmailTargets();
      if (!targets.length) { await this.log('Không có mail mới nào cần xử lý.', 'warning'); return []; }
      await this.log(`Bắt đầu xử lý ${targets.length} mail mới...`, 'info');
      const rows = [];
      for (const t of targets) {
        const r = await this.processSingleTarget(t);
        if (r) rows.push(r);
      }
      return rows;
    } finally { await this.closeContext(); }
  }

  async setupLogin() {
    await this.launchContext();
    await this.page.goto(this.account.gmailUrl);
    await ask(`[${this.account.email}] Đăng nhập xong nhấn Enter...`);
    await this.closeContext();
  }
}

// --- WORKER ENTRY ---
if (!isMainThread) {
  const run = async () => {
    const bot = new GmailWebBot(workerData.account, workerData.accountIndex);
    try {
      const mails = await bot.runAccount();
      parentPort.postMessage({ type: 'done', mails, index: workerData.accountIndex, email: workerData.account.email });
    } catch (e) {
      parentPort.postMessage({ type: 'error', error: e.message, email: workerData.account.email });
    }
  };
  run();
}

// --- MAIN FUNCTION ---
async function main() {
  banner();
  ensureDir(path.join(ROOT_DIR, 'state'));
  ensureDir(path.dirname(OUTPUT_EXCEL));

  if (process.argv.includes('--setup')) {
    const accs = loadAccounts();
    for (let i = 0; i < accs.length; i++) await new GmailWebBot(accs[i], i).setupLogin();
    return;
  }

  while (true) {
    const accounts = loadAccounts();
    const selected = loadSelectedAccounts(accounts);
    if (!selected.length) { console.log(chalk.red('Không có tài khoản được chọn. Quay lại bước setup.')); break; }

    console.log(chalk.yellow('\n--- KIỂM TRA DỮ LIỆU ĐÃ CÓ ---'));
    for (const acc of selected) {
      const st = safeJsonRead(path.join(ROOT_DIR, 'state', `${acc.id}_processed.json`), { keys: {} });
      console.log(chalk.gray(`[${acc.email}] Đã xử lý: ${Object.keys(st.keys).length} mail.`));
    }

    const targetPref = await ask(`\nBạn muốn lấy thêm bao nhiêu mail MỚI? (Nhập số hoặc 'all'): `);
    const forcePref = await ask('Có xử lý lại các mail đã từng lấy không? (y/n): ');
    
    const count = targetPref === 'all' ? 9999 : (parseInt(targetPref) || 20);
    const force = forcePref.toLowerCase() === 'y';
    process.env.HEADLESS = 'true';

    await ask(chalk.green('\nNhấn Enter để BẮT ĐẦU phiên này... '));
    const writer = new ExcelWriter(OUTPUT_EXCEL);
    let currentIdx = 0;

    while (currentIdx < selected.length) {
      const batch = selected.slice(currentIdx, currentIdx + MAX_ACCOUNT_THREADS);
      const promises = batch.map((acc, i) => {
        acc.maxEmails = count;
        const w = new Worker(__filename, { workerData: { account: acc, accountIndex: currentIdx + i, forceProcess: force } });
        return new Promise(res => {
          w.on('message', m => { if (m.type === 'done') writer.writeMails(m.mails); res(); });
          w.on('error', () => res());
        });
      });
      await Promise.all(promises);
      currentIdx += MAX_ACCOUNT_THREADS;
    }
    console.log(chalk.green('\n[!] Đã hoàn thành phiên. Quay lại vòng lặp...'));
  }
}

if (isMainThread) {
  main().catch(err => { console.error(chalk.red('Lỗi main:'), err); process.exit(1); });
}
