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
const HEADLESS = String(process.env.HEADLESS || 'false').toLowerCase() === 'true';
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

function ensureDir(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function fileExists(filePath) {
  try {
    fs.accessSync(filePath, fs.constants.F_OK);
    return true;
  } catch (_) {
    return false;
  }
}

function safeJsonRead(filePath, fallback) {
  try {
    if (!fileExists(filePath)) return fallback;
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch (_) {
    return fallback;
  }
}

function safeJsonWrite(filePath, data) {
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8');
}

function cleanText(value) {
  return String(value || '')
    .replace(/\u200b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function escapeFormula(value) {
  const text = String(value == null ? '' : value);
  return /^[=+\-@]/.test(text) ? `'${text}` : text;
}

function chunkArray(items, size) {
  const chunks = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function createInterface() {
  return readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });
}

function ask(question) {
  const rl = createInterface();
  return new Promise(resolve => {
    rl.question(question, answer => {
      rl.close();
      resolve(answer);
    });
  });
}

function banner() {
  console.log(chalk.magenta('============================================================'));
  console.log(chalk.cyan.bold(`      GMAIL WEB GEMINI BOT - Script by ${INTRO_NAME}`));
  console.log(chalk.magenta('============================================================'));
}

function loadAccounts() {
  if (!fileExists(ACCOUNTS_FILE)) {
    throw new Error(`Không tìm thấy ${ACCOUNTS_FILE}. Hãy tạo từ accounts.sample.json`);
  }

  const accounts = safeJsonRead(ACCOUNTS_FILE, []);
  if (!Array.isArray(accounts) || accounts.length === 0) {
    throw new Error('accounts.json rỗng hoặc sai định dạng.');
  }

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
  if (!fileExists(SELECTED_ACCOUNTS_FILE)) {
    return allAccounts.filter(acc => acc.enabled);
  }

  const selectedIds = fs.readFileSync(SELECTED_ACCOUNTS_FILE, 'utf8')
    .replace(/\r/g, '')
    .split('\n')
    .map(item => item.trim())
    .filter(Boolean);

  if (selectedIds.length === 0) {
    return allAccounts.filter(acc => acc.enabled);
  }

  return allAccounts.filter(acc => acc.enabled && selectedIds.includes(acc.id));
}

class ExcelWriter {
  constructor(outputFile) {
    this.outputFile = outputFile;
    this.queue = Promise.resolve();
    ensureDir(path.dirname(this.outputFile));
  }

  async initWorkbook() {
    const workbook = new ExcelJS.Workbook();
    if (fileExists(this.outputFile)) {
      await workbook.xlsx.readFile(this.outputFile);
    }

    let sheet = workbook.getWorksheet('emails');
    if (!sheet) {
      sheet = workbook.addWorksheet('emails');
      sheet.addRow(EXCEL_HEADERS);
      sheet.getRow(1).font = { bold: true };
      sheet.columns = EXCEL_HEADERS.map(header => ({ header, key: header, width: 24 }));
    }

    return { workbook, sheet };
  }

  appendRows(rows) {
    this.queue = this.queue.then(async () => {
      if (!rows || rows.length === 0) return;
      const { workbook, sheet } = await this.initWorkbook();

      for (const row of rows) {
        const excelRow = EXCEL_HEADERS.map(key => escapeFormula(row[key] ?? ''));
        sheet.addRow(excelRow);
      }

      await workbook.xlsx.writeFile(this.outputFile);
    });

    return this.queue;
  }
}

class GmailWebBot {
  constructor(account, accountIndex) {
    this.account = account;
    this.accountIndex = accountIndex;
    this.profileDir = path.resolve(ROOT_DIR, account.profileDir);
    this.stateFile = path.join(ROOT_DIR, 'state', `${account.id}_processed.json`);
    this.browserContext = null;
    this.page = null;
    this.processed = safeJsonRead(this.stateFile, {
      keys: {},
      updatedAt: null
    });
    ensureDir(path.dirname(this.stateFile));
    ensureDir(this.profileDir);
    this.userAgents = [
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    ];
  }

  getRandomUserAgent() {
    return this.userAgents[Math.floor(Math.random() * this.userAgents.length)];
  }

  async randomDelay(min = 900, max = 2200) {
    const delay = Math.floor(Math.random() * (max - min + 1)) + min;
    return sleep(delay);
  }

  async waitForAnySelector(page, selectors, timeout = 15000) {
    try {
      const promises = selectors.map(s => page.waitForSelector(s, { state: 'attached', timeout }).catch(() => null));
      const res = await Promise.race(promises);
      if (res) return true;
      // Double check if any are present
      for (const s of selectors) {
        if (await page.locator(s).count() > 0) return true;
      }
      return false;
    } catch (_) {
      return false;
    }
  }

  async captureDebug(label = 'debug') {
    try {
      const timestamp = DateTime.now().toFormat('yyyyMMdd-HHmmss');
      const filename = `${label}-${this.account.id}-${timestamp}`;
      const debugDir = path.join(ROOT_DIR, 'debug');
      ensureDir(debugDir);

      const screenshotPath = path.join(debugDir, `${filename}.png`);
      const htmlPath = path.join(debugDir, `${filename}.html`);

      await this.page.screenshot({ path: screenshotPath, fullPage: true });
      const content = await this.page.content();
      fs.writeFileSync(htmlPath, content, 'utf8');

      await this.log(`Đã lưu debug screenshot và HTML: ${filename}`, 'warning');
    } catch (e) {
      await this.log(`Không thể lưu debug: ${e.message}`, 'error');
    }
  }

  async log(msg, type = 'info') {
    const prefix = `[Tài khoản ${this.accountIndex + 1} - ${this.account.email}]`;
    let output = `${prefix} ${msg}`;
    if (type === 'success') output = chalk.green(output);
    else if (type === 'error') output = chalk.red(output);
    else if (type === 'warning') output = chalk.yellow(output);
    else output = chalk.blue(output);
    console.log(output);
  }

  saveProcessed() {
    this.processed.updatedAt = new Date().toISOString();
    safeJsonWrite(this.stateFile, this.processed);
  }

  async dismissPopups() {
    const popupSelectors = [
      'button[aria-label="Đóng"]',
      'button[aria-label="No thanks"]',
      'button[aria-label="Not now"]',
      'div[role="dialog"] button',
      '.th[role="alert"] .asb', // "Turn on notifications"
      'div[role="alertdialog"] button'
    ];

    for (const selector of popupSelectors) {
      try {
        const btn = this.page.locator(selector).first();
        if (await btn.isVisible()) {
          await this.log(`Phát hiện popup, đang đóng: ${selector}`, 'info');
          await btn.click();
          await this.randomDelay(500, 1000);
        }
      } catch (_) {}
    }
  }

  buildLaunchOptions() {
    const args = [
      '--disable-blink-features=AutomationControlled',
      '--start-maximized',
      '--lang=vi-VN'
    ];

    if (this.account.proxy) {
      args.push(`--proxy-server=${this.account.proxy}`);
    }

    // Luôn lấy giá trị mới nhất từ env vì có thể user đã chọn ở prompt
    const isHeadless = String(process.env.HEADLESS || 'false').toLowerCase() === 'true';

    return {
      headless: isHeadless,
      channel: 'chromium',
      userAgent: this.getRandomUserAgent(),
      viewport: null,
      locale: 'vi-VN',
      timezoneId: TIMEZONE,
      args
    };
  }

  async launchContext() {
    this.browserContext = await chromium.launchPersistentContext(this.profileDir, this.buildLaunchOptions());
    this.page = this.browserContext.pages()[0] || await this.browserContext.newPage();
    this.page.setDefaultTimeout(30000);
  }

  async closeContext() {
    try {
      if (this.browserContext) {
        await this.browserContext.close();
      }
    } catch (_) {}
  }

  async isLoggedIn() {
    await this.page.goto('https://mail.google.com/', { waitUntil: 'domcontentloaded' });
    await this.randomDelay();

    const url = this.page.url();
    if (url.includes('accounts.google.com')) {
      return false;
    }

    try {
      await this.page.waitForSelector('div[role="main"], table[role="grid"], input[placeholder*="Search"], input[aria-label*="Search"]', { timeout: 15000 });
      return true;
    } catch (_) {
      return !this.page.url().includes('accounts.google.com');
    }
  }

  async setupLogin() {
    await this.launchContext();
    await this.page.goto(this.account.gmailUrl, { waitUntil: 'domcontentloaded' });
    await this.log('Đã mở Gmail profile. Hãy đăng nhập thủ công rồi nhấn Enter ở terminal.', 'info');
    await ask(`[${this.account.email}] Sau khi đăng nhập xong, nhấn Enter để tiếp tục... `);

    const ok = await this.isLoggedIn();
    if (!ok) {
      await this.closeContext();
      throw new Error('Chưa phát hiện inbox Gmail sau khi login.');
    }

    await this.log('Đăng nhập Gmail thành công, session đã được lưu vào profile.', 'success');
    await this.closeContext();
  }

  async gotoInbox() {
    await this.page.goto(this.account.gmailUrl, { waitUntil: 'domcontentloaded' });
    await this.randomDelay(2000, 4000);
    await this.page.waitForSelector('body', { timeout: 30000 });

    if (this.page.url().includes('accounts.google.com')) {
      throw new Error('Profile chưa đăng nhập Gmail. Chạy npm run setup trước.');
    }

    const inboxReady = await this.waitForAnySelector(this.page, [
      'tr[role="row"]',
      'table[role="grid"] tr',
      'div[role="main"] tr.zA',
      'div[role="main"] [data-legacy-thread-id]',
      'div[role="main"]'
    ], 25000);

    if (!inboxReady) {
      await this.captureDebug('inbox-not-ready');
      await this.log('Cảnh báo: Không tìm thấy danh sách mail rõ ràng. Đang thử đóng popup và reload...', 'warning');
      await this.dismissPopups();
      await this.page.reload({ waitUntil: 'domcontentloaded' });
      await this.randomDelay(3000, 5000);
    } else {
      await this.dismissPopups();
    }
  }

  async applySearchQuery() {
    if (!this.account.query) return;

    const selectors = [
      'input[placeholder*="Search"]',
      'input[aria-label*="Search"]',
      'input[placeholder*="Tìm kiếm"]',
      'input[aria-label*="Tìm kiếm"]',
      'form[role="search"] input',
      'input[name="q"]',
      'input[aria-label="Search mail"]',
      'input[placeholder="Search mail"]',
      '.gb_He input', // Top bar search
      'header input'
    ];

    let searchBox = null;
    for (const selector of selectors) {
      const loc = this.page.locator(selector).first();
      if (await loc.count() > 0) {
        searchBox = loc;
        break;
      }
    }

    if (!searchBox) {
      await this.log('Không tìm thấy ô search Gmail, bỏ qua query filter.', 'warning');
      return;
    }

    await searchBox.click({ timeout: 10000 });
    await searchBox.fill('');
    await searchBox.type(this.account.query, { delay: 40 });
    await this.page.keyboard.press('Enter');
    await this.randomDelay(1500, 2800);
  }

  async collectLatestEmailTargets() {
    const targetCount = this.account.maxEmails;
    const onlyUnread = this.account.onlyUnread;
    const targets = [];
    const seen = new Set();
    let noGrowthRounds = 0;

    while (targets.length < targetCount && noGrowthRounds < 4) {
      const batch = await this.page.evaluate(({ onlyUnread }) => {
        // Tìm tất cả các element có data-thread-id hoặc data-legacy-thread-id
        const threadElements = Array.from(document.querySelectorAll('[data-thread-id], [data-legacy-thread-id], .zA, tr[role="row"]'));
        const items = [];
        const seenHrefs = new Set();
        const seenThreadIds = new Set();

        for (const el of threadElements) {
          const threadId = el.getAttribute('data-thread-id') || el.getAttribute('data-legacy-thread-id');
          // Tìm row cha
          let row = el.closest('tr, div[role="row"], div[role="listitem"], .zA');
          if (!row) continue;

          // Tìm link để lấy href
          const link = row.querySelector('a[href*="/"]');
          let href = link ? link.getAttribute('href') : '';
          
          if (!href && threadId) {
             // Fallback href if we have threadId
             href = `#inbox/${threadId.replace('#thread-f:', '')}`;
          }
          if (!href) continue;

          // Kiểm tra xem đã xử lý thread này trong cùng batch chưa
          const uniqueId = threadId || href;
          if (seenThreadIds.has(uniqueId)) continue;
          seenThreadIds.add(uniqueId);

          const unread = row.classList.contains('zE') || 
                         !!row.querySelector('span[aria-label*="Unread"], img[alt*="Unread"], div[aria-label*="Unread"], b') ||
                         (row.style.fontWeight === 'bold');

          if (onlyUnread && !unread) continue;

          const text = (row.innerText || '').replace(/\s+/g, ' ').trim();
          const subjectCandidate = text.split('\n').slice(0, 8).join(' | ');

          items.push({ href, preview: subjectCandidate, unread });
        }
        return items;
      }, { onlyUnread });

      const before = targets.length;
      for (const item of batch) {
        const url = item.href.startsWith('http') ? item.href : `https://mail.google.com/mail/u/0/${item.href.replace(/^\//, '')}`;
        if (!seen.has(url)) {
          seen.add(url);
          targets.push({ ...item, url });
          if (targets.length >= targetCount) break;
        }
      }

      if (targets.length === before) noGrowthRounds += 1;
      else noGrowthRounds = 0;

      if (targets.length < targetCount) {
        await this.page.mouse.wheel(0, 4000);
        await this.randomDelay(1200, 2200);
      }
    }

    return targets.slice(0, targetCount);
  }

  async extractMailDetailFromNewPage(target) {
    const detailPage = await this.browserContext.newPage();
    detailPage.setDefaultTimeout(30000);

    try {
      await detailPage.goto(target.url, { waitUntil: 'domcontentloaded' });
      await this.randomDelay(1200, 2500);

      const detail = await detailPage.evaluate(() => {
        function textOf(selectorList) {
          for (const selector of selectorList) {
            const el = document.querySelector(selector);
            if (el && el.textContent) {
              const text = el.textContent.replace(/\s+/g, ' ').trim();
              if (text) return text;
            }
          }
          return '';
        }

        function attrOf(selectorList, attr) {
          for (const selector of selectorList) {
            const el = document.querySelector(selector);
            if (el) {
              const value = el.getAttribute(attr);
              if (value) return value;
            }
          }
          return '';
        }

        const subject = textOf(['h2.hP', 'h2[data-thread-perm-id]', 'h2']);
        const senderName = textOf(['span.gD', 'h3.iw span[email]', 'span[email]']);
        const senderEmail = attrOf(['span.gD[email]', 'span[email]'], 'email') || textOf(['span[email]']);
        const rawTimestamp = attrOf(['span.g3[title]', 'span[title][class*="g3"]'], 'title') || textOf(['span.g3', 'span[title][class*="g3"]', 'time']);

        let body = '';
        const bodyNodes = Array.from(document.querySelectorAll('div.a3s.aiL, div.a3s, div[role="listitem"] div[dir="auto"]'));
        if (bodyNodes.length > 0) {
          const longest = bodyNodes
            .map(node => (node.innerText || '').trim())
            .sort((a, b) => b.length - a.length)[0] || '';
          body = longest;
        }

        if (!body) {
          body = (document.body.innerText || '').replace(/\s+/g, ' ').trim();
        }

        return {
          subject,
          senderName,
          senderEmail,
          rawTimestamp,
          body,
          pageUrl: location.href
        };
      });

      return detail;
    } finally {
      await detailPage.close().catch(() => {});
    }
  }

  buildMessageKey(detail) {
    return cleanText([
      this.account.id,
      detail.senderEmail || detail.senderName,
      detail.subject,
      detail.rawTimestamp,
      (detail.body || '').slice(0, 200)
    ].join('|')).toLowerCase();
  }

  normalizeTimestamp(rawTimestamp) {
    const now = DateTime.now().setZone(TIMEZONE);
    let dt = DateTime.fromISO(rawTimestamp, { zone: TIMEZONE });

    if (!dt.isValid) {
      dt = DateTime.fromRFC2822(rawTimestamp, { zone: TIMEZONE });
    }

    if (!dt.isValid) {
      dt = DateTime.fromFormat(rawTimestamp, 'ccc, LLL d, yyyy, h:mm a', { zone: TIMEZONE, locale: 'en' });
    }

    if (!dt.isValid) {
      dt = DateTime.fromJSDate(new Date(rawTimestamp), { zone: TIMEZONE });
    }

    if (!dt.isValid) {
      dt = now;
    }

    return {
      raw: rawTimestamp || '',
      iso: dt.toISO(),
      local: dt.toFormat('dd/MM/yyyy HH:mm:ss')
    };
  }

  async saveEmailLocally(detail, accountEmail) {
    try {
      const dt = this.normalizeTimestamp(detail.rawTimestamp);
      const iso = DateTime.fromISO(dt.iso);
      const year = iso.toFormat('yyyy');
      const month = iso.toFormat('MM');
      const day = iso.toFormat('dd');
      
      const safeSubject = (detail.subject || 'no-subject')
        .replace(/[\\/:*?"<>|]/g, '_')
        .slice(0, 100);
      
      const fileName = `${iso.toFormat('HHmmss')}_${safeSubject}.txt`;
      const dirPath = path.join(ROOT_DIR, 'output', 'emails', accountEmail, year, month, day);
      ensureDir(dirPath);
      
      const filePath = path.join(dirPath, fileName);
      const content = [
        `Account: ${accountEmail}`,
        `Subject: ${detail.subject}`,
        `From: ${detail.senderName} <${detail.senderEmail}>`,
        `Date Raw: ${detail.rawTimestamp}`,
        `Date ISO: ${dt.iso}`,
        `URL: ${detail.pageUrl}`,
        `----------------------------------------`,
        detail.body
      ].join('\n');
      
      fs.writeFileSync(filePath, content, 'utf8');
      return filePath;
    } catch (e) {
      await this.log(`Lỗi lưu file email: ${e.message}`, 'error');
      return null;
    }
  }

  async callGemini(detail) {
    if (!GEMINI_API_KEY) {
      return {
        ai_status: 'skipped',
        ai_confidence: '',
        ai_json: '',
        ai_error: 'Missing GEMINI_API_KEY',
        request_type: '',
        customer_name: '',
        phone: '',
        email_extracted: '',
        order_code: '',
        amount: '',
        currency: '',
        note: ''
      };
    }

    const prompt = [
      'Bạn là bộ chuẩn hóa email.',
      'Đọc email và trả về JSON hợp lệ đúng schema dưới đây.',
      'Chỉ trả về JSON, không giải thích, không markdown.',
      '{',
      '  "request_type": "",',
      '  "customer_name": "",',
      '  "phone": "",',
      '  "email_extracted": "",',
      '  "order_code": "",',
      '  "amount": "",',
      '  "currency": "",',
      '  "note": "",',
      '  "confidence": 0',
      '}',
      '',
      `From: ${detail.senderName} <${detail.senderEmail}>`,
      `Subject: ${detail.subject}`,
      `Received: ${detail.rawTimestamp}`,
      `Body: ${detail.body}`
    ].join('\n');

    try {
      const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(GEMINI_MODEL)}:generateContent?key=${encodeURIComponent(GEMINI_API_KEY)}`;
      const response = await axios.post(url, {
        contents: [{ parts: [{ text: prompt }] }],
        generationConfig: {
          temperature: 0.1,
          responseMimeType: 'application/json'
        }
      }, {
        timeout: 60000,
        headers: { 'Content-Type': 'application/json' }
      });

      const text = response?.data?.candidates?.[0]?.content?.parts?.map(part => part.text || '').join('') || '{}';
      const parsed = JSON.parse(text);

      return {
        ai_status: 'success',
        ai_confidence: parsed.confidence ?? '',
        ai_json: JSON.stringify(parsed),
        ai_error: '',
        request_type: parsed.request_type || '',
        customer_name: parsed.customer_name || '',
        phone: parsed.phone || '',
        email_extracted: parsed.email_extracted || '',
        order_code: parsed.order_code || '',
        amount: parsed.amount || '',
        currency: parsed.currency || '',
        note: parsed.note || ''
      };
    } catch (error) {
      return {
        ai_status: 'error',
        ai_confidence: '',
        ai_json: '',
        ai_error: cleanText(error.response?.data ? JSON.stringify(error.response.data) : error.message),
        request_type: '',
        customer_name: '',
        phone: '',
        email_extracted: '',
        order_code: '',
        amount: '',
        currency: '',
        note: ''
      };
    }
  }

  async processSingleTarget(target) {
    const detail = await this.extractMailDetailFromNewPage(target);
    const messageKey = this.buildMessageKey(detail);

    if (this.processed.keys[messageKey] && !workerData.forceProcess) {
      await this.log(`Bỏ qua mail trùng: ${detail.subject || target.preview}`, 'warning');
      return null;
    }

    // 1. Lưu mail ra file local trước
    await this.saveEmailLocally(detail, this.account.email);

    // 2. Normalize và gọi AI
    const normalizedTime = this.normalizeTimestamp(detail.rawTimestamp);
    const ai = await this.callGemini(detail);

    const row = {
      account_id: this.account.id,
      email: this.account.email,
      message_key: messageKey,
      gmail_url: this.account.gmailUrl,
      thread_url: detail.pageUrl || target.url,
      sender_name: detail.senderName || '',
      sender_email: detail.senderEmail || '',
      subject: detail.subject || '',
      received_at_raw: normalizedTime.raw,
      received_at_iso: normalizedTime.iso,
      received_at_local: normalizedTime.local,
      body_text: detail.body || '',
      ai_json: ai.ai_json,
      ai_status: ai.ai_status,
      ai_confidence: ai.ai_confidence,
      ai_error: ai.ai_error,
      request_type: ai.request_type,
      customer_name: ai.customer_name,
      phone: ai.phone,
      email_extracted: ai.email_extracted,
      order_code: ai.order_code,
      amount: ai.amount,
      currency: ai.currency,
      note: ai.note,
      created_at: DateTime.now().setZone(TIMEZONE).toISO()
    };

    this.processed.keys[messageKey] = {
      subject: row.subject,
      sender_email: row.sender_email,
      received_at_iso: row.received_at_iso,
      saved_at: row.created_at
    };

    this.saveProcessed();
    await this.log(`Đã xử lý mail: ${row.subject || target.preview}`, ai.ai_status === 'success' ? 'success' : 'warning');
    return row;
  }

  async processTargets(targets) {
    const chunks = chunkArray(targets, Math.max(1, this.account.mailConcurrency));
    const rows = [];

    for (const chunk of chunks) {
      const results = await Promise.allSettled(chunk.map(target => this.processSingleTarget(target)));
      for (const result of results) {
        if (result.status === 'fulfilled' && result.value) {
          rows.push(result.value);
        } else if (result.status === 'rejected') {
          await this.log(`Lỗi xử lý mail: ${result.reason.message}`, 'error');
        }
      }
      await this.randomDelay(800, 1800);
    }

    return rows;
  }

  async runAccount() {
    try {
      await this.launchContext();
      await this.gotoInbox();
      
      // Đảm bảo là đang ở Inbox
      try {
        const inboxLink = this.page.locator('a[aria-label*="Hộp thư đến"], a[title*="Hộp thư đến"], a[href*="#inbox"]').first();
        if (await inboxLink.isVisible()) {
          await inboxLink.click();
          await this.randomDelay(1000, 2000);
        }
      } catch (_) {}

      await this.applySearchQuery();

      const targets = await this.collectLatestEmailTargets();
      if (!targets.length) {
        await this.captureDebug('no-mails-found');
        await this.log('Không tìm thấy mail nào phù hợp.', 'warning');
        return [];
      }

      await this.log(`Đã thu thập ${targets.length} mail mục tiêu. Bắt đầu xử lý...`, 'info');
      const rows = await this.processTargets(targets);
      await this.log(`Hoàn thành tài khoản. Mail mới ghi Excel: ${rows.length}`, 'success');
      return rows;
    } finally {
      await this.closeContext();
    }
  }
}

async function runWorker(data) {
  const bot = new GmailWebBot(data.account, data.accountIndex);
  try {
    const rows = await Promise.race([
      bot.runAccount(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Worker timeout sau 30 phút')), 30 * 60 * 1000))
    ]);
    parentPort.postMessage({ accountIndex: data.accountIndex, rows });
  } catch (error) {
    parentPort.postMessage({ accountIndex: data.accountIndex, error: error.message });
  }
}

async function setupMode(accounts) {
  banner();
  console.log(chalk.yellow('Chế độ setup: mở Gmail profile để bạn đăng nhập thủ công.'));

  for (let i = 0; i < accounts.length; i++) {
    const account = accounts[i];
    const bot = new GmailWebBot(account, i);
    try {
      await bot.setupLogin();
    } catch (error) {
      console.log(chalk.red(`[${account.email}] Setup thất bại: ${error.message}`));
    }
  }

  console.log(chalk.green('Setup hoàn tất. Giờ có thể chạy npm start.'));
}

async function countdown(seconds) {
  for (let i = Math.floor(seconds); i >= 0; i--) {
    readline.cursorTo(process.stdout, 0);
    process.stdout.write(chalk.cyan(`[*] Chờ ${i} giây để tiếp tục...`));
    await sleep(1000);
  }
  console.log('');
}

async function main() {
  banner();
  ensureDir(path.join(ROOT_DIR, 'state'));
  ensureDir(path.dirname(OUTPUT_EXCEL));

  const accounts = loadAccounts();
  const selectedAccounts = loadSelectedAccounts(accounts);

  if (!selectedAccounts.length) {
    throw new Error('Không có tài khoản nào được chọn để chạy.');
  }

  if (process.argv.includes('--setup')) {
    await setupMode(selectedAccounts);
    return;
  }

  // --- HIỆN TRẠNG THÁI ---
  console.log(chalk.yellow('\n--- KIỂM TRA DỮ LIỆU ĐÃ CÓ ---'));
  for (const acc of selectedAccounts) {
    const stateFile = path.join(ROOT_DIR, 'state', `${acc.id}_processed.json`);
    const state = safeJsonRead(stateFile, { keys: {} });
    const count = Object.keys(state.keys).length;
    console.log(chalk.gray(`[${acc.email}] Đã xử lý: ${count} mail.`));
  }

  // --- HỎI NGƯỜI DÙNG QUY TRÌNH ---
  console.log(chalk.yellow('\n--- THIẾT LẬP PHIÊN CHẠY ---'));
  const targetCountInput = await ask(`Bạn muốn lấy thêm bao nhiêu mail mỗi tài khoản? (Nhập số, hoặc 'all'): `);
  const reprocessInput = await ask('Bạn có muốn xử lý lại các mail đã từng lấy không? (y/n): ');

  const targetCount = targetCountInput.toLowerCase() === 'all' ? 9999 : (parseInt(targetCountInput) || 20);
  const forceProcess = reprocessInput.toLowerCase() === 'y';

  // Chạy ẩn trình duyệt theo mặc định
  const runHeadless = true;
  process.env.HEADLESS = 'true';

  console.log(chalk.cyan(`\n[*] Sẽ lấy tối đa ${targetCount} mail mới.`));
  console.log(chalk.cyan(`[*] Khử trùng: ${forceProcess ? 'Tắt (Lấy lại tất cả)' : 'Bật (Chỉ lấy mail chưa có)'}`));
  console.log(chalk.cyan(`[*] Chế độ chạy: Ẩn trình duyệt (Mặc định)`));
  
  const confirm = await ask(chalk.green('\nNhấn Enter để BẮT ĐẦU... '));

  const excelWriter = new ExcelWriter(OUTPUT_EXCEL);
  let currentIndex = 0;
  const errors = [];

  console.log(chalk.magenta('Đã sợ thì đừng dùng, đã dùng thì đừng sợ!'));
  console.log(chalk.blue(`Số tài khoản được chọn: ${selectedAccounts.length}`));
  console.log(chalk.blue(`Excel đầu ra: ${OUTPUT_EXCEL}`));

  while (currentIndex < selectedAccounts.length) {
    const workerPromises = [];
    const batchSize = Math.min(MAX_ACCOUNT_THREADS, selectedAccounts.length - currentIndex);

    for (let i = 0; i < batchSize; i++) {
      const account = selectedAccounts[currentIndex];
      // Override account maxEmails with user input
      account.maxEmails = targetCount;

      const worker = new Worker(__filename, {
        workerData: {
          account,
          accountIndex: currentIndex,
          forceProcess
        }
      });

      workerPromises.push(new Promise(resolve => {
        worker.on('message', async message => {
          if (message.error) {
            errors.push(`Tài khoản ${message.accountIndex + 1}: ${message.error}`);
          } else if (message.rows && message.rows.length) {
            await excelWriter.appendRows(message.rows);
          }
          resolve();
        });

        worker.on('error', error => {
          errors.push(`Lỗi worker cho tài khoản ${currentIndex + 1}: ${error.message}`);
          resolve();
        });

        worker.on('exit', code => {
          if (code !== 0) {
            errors.push(`Worker tài khoản ${currentIndex + 1} thoát với mã ${code}`);
          }
          resolve();
        });
      }));

      currentIndex++;
    }

    await Promise.all(workerPromises);

    if (currentIndex < selectedAccounts.length) {
      await countdown(3);
    }
  }

  if (errors.length > 0) {
    console.log(chalk.red('================= DANH SÁCH LỖI ================='));
    errors.forEach(error => console.log(chalk.red(error)));
  }

  console.log(chalk.green('Hoàn tất toàn bộ tiến trình.'));
}

if (isMainThread) {
  main().catch(error => {
    console.error(chalk.red(`Lỗi rồi: ${error.message}`));
    process.exit(1);
  });
} else {
  runWorker(workerData);
}
