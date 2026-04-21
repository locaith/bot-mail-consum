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
const TELEGRAM_MODE = String(process.env.TELEGRAM_MODE || 'false').toLowerCase() === 'true';
const TELEGRAM_SELECTED_EMAIL = cleanText(process.env.TELEGRAM_SELECTED_EMAIL || '').toLowerCase();
const TELEGRAM_TARGET_COUNT = cleanText(process.env.TELEGRAM_TARGET_COUNT || 'all');
const TELEGRAM_FORCE_REPROCESS = String(process.env.TELEGRAM_FORCE_REPROCESS || 'false').toLowerCase() === 'true';
const TELEGRAM_CUSTOM_PROMPT = cleanText(process.env.TELEGRAM_CUSTOM_PROMPT || '');
const TELEGRAM_LOGIN_PASSWORD = process.env.TELEGRAM_LOGIN_PASSWORD || '';
const TELEGRAM_WAIT_APPROVAL_SECONDS = parseInt(process.env.TELEGRAM_WAIT_APPROVAL_SECONDS || '240', 10);
const TELEGRAM_JSON_PREFIX = '__TELEGRAM_JSON__';

let EXCEL_HEADERS = [
  'account_id', 'email', 'sender_email', 'subject',
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

function emitTelegramEvent(event, message, extra = {}) {
  if (!TELEGRAM_MODE) return;
  const payload = {
    event: cleanText(event),
    message: cleanText(message),
    ...extra
  };
  console.log(`${TELEGRAM_JSON_PREFIX}${JSON.stringify(payload)}`);
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
    return [];
  }

  const accounts = safeJsonRead(ACCOUNTS_FILE, []);
  if (!Array.isArray(accounts) || accounts.length === 0) {
    return [];
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
    }).catch(err => {
      console.error(`\n[LỖI EXCEL] Không thể lưu file ${this.outputFile}. Có thể file đang mở. Lỗi: ${err.message}\n`);
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
      } catch (_) { }
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
    } catch (_) { }
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

  async setupLoginAutomated(password, waitSeconds = 240) {
    if (!password) {
      throw new Error('Thiếu mật khẩu Gmail cho chế độ tự động.');
    }
    await this.launchContext();
    await this.page.goto(this.account.gmailUrl, { waitUntil: 'domcontentloaded' });
    emitTelegramEvent('login_started', `Tôi đã mở phiên đăng nhập cho ${this.account.email}.`);
    await this.randomDelay(800, 1600);

    if (await this.isLoggedIn()) {
      emitTelegramEvent(
        'login_ready',
        `Phiên Gmail của ${this.account.email} đã đăng nhập sẵn, tôi chuyển sang lấy mail.`,
        { email: this.account.email, profile_dir: this.profileDir }
      );
      await this.closeContext();
      return;
    }

    const emailInput = this.page.locator('input[type="email"]').first();
    if (await emailInput.count() > 0) {
      await emailInput.fill(this.account.email, { timeout: 15000 });
      const nextButton = this.page.locator('#identifierNext button, #identifierNext').first();
      if (await nextButton.count() > 0) {
        await nextButton.click({ timeout: 15000 });
      } else {
        await this.page.keyboard.press('Enter');
      }
      await this.randomDelay(1200, 2200);
      emitTelegramEvent('login_email_filled', `Tôi đã điền địa chỉ ${this.account.email}.`);
    }

    const passwordInput = this.page.locator('input[type="password"]').first();
    await passwordInput.waitFor({ timeout: 30000 });
    await passwordInput.fill(password, { timeout: 15000 });
    const passwordNext = this.page.locator('#passwordNext button, #passwordNext').first();
    if (await passwordNext.count() > 0) {
      await passwordNext.click({ timeout: 15000 });
    } else {
      await this.page.keyboard.press('Enter');
    }
    emitTelegramEvent(
      'approval_requested',
      'Tôi đã nhập mật khẩu. Nếu điện thoại hiện xác thực, hãy chấp thuận để tôi tiếp tục.',
      { email: this.account.email }
    );

    const deadline = Date.now() + Math.max(60, waitSeconds) * 1000;
    let lastHeartbeat = 0;
    while (Date.now() < deadline) {
      await this.randomDelay(3000, 5000);
      if (await this.isLoggedIn()) {
        emitTelegramEvent(
          'login_ready',
          `Đăng nhập Gmail cho ${this.account.email} đã thành công và session đã được lưu.`,
          { email: this.account.email, profile_dir: this.profileDir }
        );
        await this.closeContext();
        return;
      }
      if (Date.now() - lastHeartbeat >= 15000) {
        emitTelegramEvent(
          'approval_waiting',
          `Tôi vẫn đang chờ xác thực trên điện thoại cho ${this.account.email}.`,
          { email: this.account.email }
        );
        lastHeartbeat = Date.now();
      }
    }

    emitTelegramEvent(
      'password_required',
      `Tôi vẫn chưa vào được hộp thư ${this.account.email}. Hãy kiểm tra lại xác thực trên điện thoại hoặc gửi lại mật khẩu nếu cần.`,
      { email: this.account.email, profile_dir: this.profileDir }
    );
    await this.closeContext();
    throw new Error('awaiting_phone_approval_or_login_timeout');
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

  async extractMailDetailFromNewPage(target, retries = 3) {
    for (let attempt = 1; attempt <= retries; attempt++) {
      let detailPage;
      try {
        detailPage = await this.browserContext.newPage();
        detailPage.setDefaultTimeout(30000);

        await detailPage.goto(target.url, { waitUntil: 'domcontentloaded' });
        // Đợi DOM của nội dung email load thành công để tránh lỗi cào nhầm giao diện loading có text "Tìm kiếm"
        await detailPage.waitForSelector('h2.hP, div.a3s', { state: 'attached', timeout: 20000 }).catch(() => {});
        await this.randomDelay(1200, 2500);

        const detail = await detailPage.evaluate(() => {
          function textOfLast(selectorList) {
            for (const selector of selectorList) {
              const els = document.querySelectorAll(selector);
              if (els.length > 0) {
                for (let i = els.length - 1; i >= 0; i--) {
                  const text = (els[i].textContent || '').replace(/\s+/g, ' ').trim();
                  if (text) return text;
                }
              }
            }
            return '';
          }

          function attrOfLast(selectorList, attr) {
            for (const selector of selectorList) {
              const els = document.querySelectorAll(selector);
              if (els.length > 0) {
                for (let i = els.length - 1; i >= 0; i--) {
                  const value = els[i].getAttribute(attr);
                  if (value) return value;
                }
              }
            }
            return '';
          }

          const subject = textOfLast(['h2.hP', 'h2[data-thread-perm-id]']);
          const senderName = textOfLast(['span.gD', 'h3.iw span[email]', 'span[email]']);
          const senderEmail = attrOfLast(['span.gD[email]', 'span[email]'], 'email') || textOfLast(['span[email]']);
          const rawTimestamp = attrOfLast(['span.g3[title]', 'span[title][class*="g3"]'], 'title') || textOfLast(['span.g3', 'span[title][class*="g3"]', 'time']);

          let body = '';
          const bodyNodes = Array.from(document.querySelectorAll('div.a3s.aiL, div.a3s, div[role="listitem"] div[dir="auto"]'));
          if (bodyNodes.length > 0) {
            // Lấy nội dung của message cuối cùng (mới nhất trong thread)
            body = (bodyNodes[bodyNodes.length - 1].innerText || '').replace(/\s+/g, ' ').trim();
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

        await detailPage.close().catch(() => { });
        return detail;
      } catch (error) {
        if (detailPage) await detailPage.close().catch(() => {});
        if (attempt === retries) throw error;
        await this.randomDelay(2000, 4000);
      }
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
    const customPrompt = workerData.customPrompt || '';
    const customColumns = workerData.customColumns || [];

    if (!GEMINI_API_KEY) {
      const skipObj = { ai_status: 'skipped', ai_confidence: '', ai_json: '', ai_error: 'Missing GEMINI_API_KEY' };
      if (customColumns.length > 0) {
        customColumns.forEach(c => skipObj[c] = '');
      } else {
        ['request_type', 'customer_name', 'phone', 'email_extracted', 'order_code', 'amount', 'currency', 'note'].forEach(c => skipObj[c] = '');
      }
      return skipObj;
    }

    let dynamicSchemaObj = {};
    if (customColumns.length > 0) {
      customColumns.forEach(c => dynamicSchemaObj[c] = "");
    } else {
      dynamicSchemaObj = {
        "request_type": "",
        "customer_name": "",
        "phone": "",
        "email_extracted": "",
        "order_code": "",
        "amount": "",
        "currency": "",
        "note": ""
      };
    }
    dynamicSchemaObj.confidence = 0;
    const dynamicSchemaStr = JSON.stringify(dynamicSchemaObj, null, 2);

    const taskText = customPrompt 
      ? `Yêu cầu đặc biệt: ${customPrompt}\n` 
      : 'Bạn là bộ chuẩn hóa email.\n';

    const prompt = [
      taskText,
      'Đọc email và trả về JSON hợp lệ đúng schema dưới đây.',
      'Chỉ trả về JSON, không giải thích, không markdown.',
      dynamicSchemaStr,
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
      
      let parsed = {};
      try {
         const jsonMatch = text.match(/```json\n([\s\S]*?)\n```/);
         if (jsonMatch) parsed = JSON.parse(jsonMatch[1]);
         else parsed = JSON.parse(text);
      } catch (e) {
         parsed = {};
      }

      const resObj = { ai_status: 'success', ai_confidence: parsed.confidence ?? '', ai_json: JSON.stringify(parsed), ai_error: '' };
      if (customColumns.length > 0) {
        customColumns.forEach(c => resObj[c] = parsed[c] || '');
      } else {
        ['request_type', 'customer_name', 'phone', 'email_extracted', 'order_code', 'amount', 'currency', 'note'].forEach(c => resObj[c] = parsed[c] || '');
      }
      return resObj;
    } catch (error) {
      const errObj = { ai_status: 'error', ai_confidence: '', ai_json: '', ai_error: cleanText(error.response?.data ? JSON.stringify(error.response.data) : error.message) };
      if (customColumns.length > 0) {
        customColumns.forEach(c => errObj[c] = '');
      } else {
        ['request_type', 'customer_name', 'phone', 'email_extracted', 'order_code', 'amount', 'currency', 'note'].forEach(c => errObj[c] = '');
      }
      return errObj;
    }
  }

  async processSingleTarget(target) {
    const detail = await this.extractMailDetailFromNewPage(target);
    const messageKey = this.buildMessageKey(detail);

    if (this.processed.keys[messageKey] && !workerData.forceProcess) {
      await this.log(`Bỏ qua mail trùng: ${target.preview}`, 'warning');
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
      created_at: DateTime.now().setZone(TIMEZONE).toISO()
    };

    const customColumns = workerData.customColumns || [];
    if (customColumns.length > 0) {
      customColumns.forEach(c => row[c] = ai[c] || '');
    } else {
      ['request_type', 'customer_name', 'phone', 'email_extracted', 'order_code', 'amount', 'currency', 'note'].forEach(c => row[c] = ai[c] || '');
    }

    this.processed.keys[messageKey] = {
      subject: row.subject,
      sender_email: row.sender_email,
      received_at_iso: row.received_at_iso,
      saved_at: row.created_at
    };

    this.saveProcessed();
    await this.log(`Đã xử lý mail: ${target.preview}`, ai.ai_status === 'success' ? 'success' : 'warning');
    return row;
  }

  async processTargets(targets) {
    const chunks = chunkArray(targets, Math.max(1, this.account.mailConcurrency));
    const rows = [];

    for (const chunk of chunks) {
      if (chunk.length > 1) {
         await this.log(`Đang chạy song song cào ${chunk.length} mail cùng lúc (Mở ${chunk.length} tab)...`, 'info');
      }
      const results = await Promise.allSettled(chunk.map(target => this.processSingleTarget(target)));
      for (const result of results) {
        if (result.status === 'fulfilled' && result.value) {
          rows.push(result.value);
        } else if (result.status === 'rejected') {
          await this.log(`Lỗi xử lý mail: ${result.reason.message || result.reason}`, 'error');
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
      } catch (_) { }

      await this.applySearchQuery();
      // Đảm bảo DOM load xong kết quả
      await this.page.waitForSelector('[data-thread-id], .zA', { state: 'attached', timeout: 15000 }).catch(() => {});

      const targetCount = this.account.maxEmails;
      const onlyUnread = this.account.onlyUnread;
      const rows = [];
      const seenUrls = new Set();
      let noGrowthRounds = 0;
      let consecutiveFoundDuplicates = 0;

      await this.log(`Bắt đầu thu thập tối đa ${targetCount} mail mới...`, 'info');

      while (rows.length < targetCount && noGrowthRounds < 4) {
        const batch = await this.page.evaluate(({ onlyUnread }) => {
          // Bắt các row email có ID nhóm (data-thread-id) để tránh bị nhầm với element gợi ý tìm kiếm
          const threadElements = Array.from(document.querySelectorAll('tr[data-thread-id], tr[data-legacy-thread-id], div[data-thread-id], div[data-legacy-thread-id], span[data-thread-id], .zA'));
          const items = [];
          const seenThreadIds = new Set();

          for (const el of threadElements) {
            const threadId = el.getAttribute('data-thread-id') || el.getAttribute('data-legacy-thread-id');
            let row = el; // Vì selector đã là .zA, row chính là email
            if (!row) continue;

            const link = row.querySelector('a[href*="/"]');
            let href = link ? link.getAttribute('href') : '';

            if (!href && threadId) {
              href = `#inbox/${threadId.replace('#thread-f:', '')}`;
            }
            if (!href) continue;

            const uniqueId = threadId || href;
            if (seenThreadIds.has(uniqueId)) continue;
            seenThreadIds.add(uniqueId);

            const unread = row.classList.contains('zE') ||
              !!row.querySelector('span[aria-label*="Unread"], img[alt*="Unread"], div[aria-label*="Unread"], b') ||
              (row.style.fontWeight === 'bold');

            if (onlyUnread && !unread) continue;

            const text = (row.innerText || '').replace(/\s+/g, ' ').trim();
            const preview = text.split('\n').slice(0, 8).join(' | ');

            items.push({ href, preview, unread });
          }
          return items;
        }, { onlyUnread });

        let newTargets = [];
        for (const item of batch) {
          const url = item.href.startsWith('http') ? item.href : `https://mail.google.com/mail/u/0/${item.href.replace(/^\//, '')}`;
          if (!seenUrls.has(url)) {
            seenUrls.add(url);
            newTargets.push({ ...item, url });
          }
        }

        if (newTargets.length === 0) {
          noGrowthRounds++;
          if (noGrowthRounds < 4) {
            await this.page.mouse.wheel(0, 4000);
            await this.randomDelay(1500, 2500);
          }
          continue;
        }

        noGrowthRounds = 0;

        const chunks = chunkArray(newTargets, Math.max(1, this.account.mailConcurrency));
        for (const chunk of chunks) {
          const results = await Promise.allSettled(chunk.map(t => this.processSingleTarget(t)));
          for (const result of results) {
            if (result.status === 'fulfilled' && result.value) {
              rows.push(result.value);
              consecutiveFoundDuplicates = 0;
              if (rows.length >= targetCount) break;
            } else if (result.status === 'fulfilled' && result.value === null) {
              consecutiveFoundDuplicates++;
            } else if (result.status === 'rejected') {
              await this.log(`Lỗi xử lý mail: ${result.reason.message}`, 'error');
            }
          }
          if (rows.length >= targetCount) break;
          await this.randomDelay(800, 1800);
        }

        if (rows.length >= targetCount) break;

        await this.page.mouse.wheel(0, 4000);
        await this.randomDelay(1500, 2500);

        if (consecutiveFoundDuplicates > 500 && !workerData.forceProcess) {
          await this.log(`Gặp quá nhiều mail đã xử lý liên tiếp (${consecutiveFoundDuplicates}), tự động dừng.`, 'warning');
          break;
        }
      }

      if (rows.length === 0) {
        await this.captureDebug('no-mails-found');
        await this.log('Không thu thập được mail mới nào.', 'warning');
        return [];
      }

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

async function handleAddAccountFlow(existingAccounts) {
  const newEmail = await ask('Nhập địa chỉ email Gmail (Ví dụ: test@gmail.com): ');
  if (!newEmail.trim()) return existingAccounts;

  const id = newEmail.split('@')[0].replace(/[^a-zA-Z0-9]/g, '_');
  const newAcc = {
    id: id,
    email: newEmail.trim(),
    maxEmails: 50,
    mailConcurrency: 10,
    profileDir: `./profiles/${id}`,
    gmailUrl: 'https://mail.google.com/mail/u/0/#inbox',
    enabled: true,
    onlyUnread: false,
    query: '',
    proxy: ''
  };
  
  existingAccounts.push(newAcc);
  safeJsonWrite(ACCOUNTS_FILE, existingAccounts);
  console.log(chalk.green(`\nĐã thêm tài khoản: ${newEmail}. Đang mở tiến trình Chrome duyệt để bạn đăng nhập lần đầu...`));
  
  await setupMode([newAcc]);
  return loadAccounts();
}

function ensureTelegramAccount(existingAccounts, email) {
  const normalizedEmail = cleanText(email).toLowerCase();
  let account = existingAccounts.find(acc => cleanText(acc.email).toLowerCase() === normalizedEmail);
  if (account) return { accounts: existingAccounts, account };

  const id = normalizedEmail.split('@')[0].replace(/[^a-zA-Z0-9]/g, '_') || 'gmail_account';
  account = {
    id,
    email: normalizedEmail,
    maxEmails: 9999,
    mailConcurrency: DEFAULT_MAIL_CONCURRENCY,
    profileDir: `./profiles/${id}`,
    gmailUrl: 'https://mail.google.com/mail/u/0/#inbox',
    enabled: true,
    onlyUnread: false,
    query: '',
    proxy: ''
  };
  existingAccounts.push(account);
  safeJsonWrite(ACCOUNTS_FILE, existingAccounts);
  return { accounts: existingAccounts, account };
}

async function runTelegramAccount(account, outputFile, targetCount, forceProcess, customPrompt) {
  const excelWriter = new ExcelWriter(outputFile);
  const rows = await new Promise((resolve, reject) => {
    const worker = new Worker(__filename, {
      workerData: {
        account: { ...account, maxEmails: targetCount },
        accountIndex: 0,
        forceProcess,
        customPrompt,
        customColumns: []
      }
    });

    worker.on('message', async message => {
      try {
        if (message.error) {
          reject(new Error(message.error));
          return;
        }
        const emittedRows = Array.isArray(message.rows) ? message.rows : [];
        if (emittedRows.length) {
          await excelWriter.appendRows(emittedRows);
        }
        emitTelegramEvent(
          'fetch_completed',
          `Tôi đã xử lý xong ${emittedRows.length} mail mới từ ${account.email}.`,
          { email: account.email, row_count: emittedRows.length, output_excel: outputFile }
        );
        resolve(emittedRows);
      } catch (error) {
        reject(error);
      }
    });

    worker.on('error', error => reject(error));
    worker.on('exit', code => {
      if (code !== 0) {
        reject(new Error(`Worker Gmail thoát với mã ${code}`));
      }
    });
  });
  await excelWriter.queue;
  return rows;
}

async function runTelegramMode() {
  banner();
  ensureDir(path.join(ROOT_DIR, 'state'));
  ensureDir(path.dirname(OUTPUT_EXCEL));

  if (!TELEGRAM_SELECTED_EMAIL) {
    emitTelegramEvent('error', 'Thiếu địa chỉ Gmail đích cho chế độ Telegram.');
    process.exit(2);
  }

  let accounts = loadAccounts();
  const ensured = ensureTelegramAccount(accounts, TELEGRAM_SELECTED_EMAIL);
  accounts = ensured.accounts;
  const account = ensured.account;
  emitTelegramEvent('start', `Tôi bắt đầu chuẩn bị kéo mail cho ${account.email}.`, {
    email: account.email,
    output_excel: OUTPUT_EXCEL,
  });

  const bot = new GmailWebBot(account, 0);
  let loggedIn = false;
  try {
    await bot.launchContext();
    loggedIn = await bot.isLoggedIn();
  } finally {
    await bot.closeContext();
  }

  if (!loggedIn) {
    if (!TELEGRAM_LOGIN_PASSWORD) {
      emitTelegramEvent(
        'password_required',
        `Tôi cần mật khẩu để đăng nhập Gmail cho ${account.email}. Sau đó tôi sẽ tự điền vào Chrome và chờ bạn xác thực trên điện thoại.`,
        { email: account.email, profile_dir: path.resolve(ROOT_DIR, account.profileDir) }
      );
      process.exit(12);
    }
    await bot.setupLoginAutomated(TELEGRAM_LOGIN_PASSWORD, TELEGRAM_WAIT_APPROVAL_SECONDS);
  } else {
    emitTelegramEvent(
      'login_ready',
      `Phiên Gmail của ${account.email} đã có sẵn. Tôi chuyển sang lấy mail ngay bây giờ.`,
      { email: account.email, profile_dir: path.resolve(ROOT_DIR, account.profileDir) }
    );
  }

  const targetCount = TELEGRAM_TARGET_COUNT.toLowerCase() === 'all'
    ? 9999
    : (parseInt(TELEGRAM_TARGET_COUNT || '0', 10) || 50);
  emitTelegramEvent(
    'fetch_started',
    `Tôi bắt đầu kéo mail từ ${account.email} và sẽ xuất toàn bộ ra file Excel khi xong.`,
    { email: account.email, target_count: targetCount, output_excel: OUTPUT_EXCEL }
  );
  await runTelegramAccount(account, OUTPUT_EXCEL, targetCount, TELEGRAM_FORCE_REPROCESS, TELEGRAM_CUSTOM_PROMPT);
  emitTelegramEvent(
    'completed',
    `Tôi đã hoàn tất việc kéo mail từ ${account.email} và lưu file Excel xong.`,
    { email: account.email, output_excel: OUTPUT_EXCEL, profile_dir: path.resolve(ROOT_DIR, account.profileDir) }
  );
}

async function runUniversalAgent(targetUrl, taskPrompt, colsInput) {
  let customColumns = [];
  if (colsInput.trim()) {
    customColumns = colsInput.split(',').map(c => cleanText(c).replace(/[^a-zA-Z0-9_\u0080-\uFFFF]/g, '')).filter(Boolean);
  }
  if (customColumns.length === 0) {
    customColumns = ['data_1', 'data_2', 'data_3'];
  }

  const outFile = path.join(ROOT_DIR, 'output', 'website_data.xlsx');
  const workbook = new ExcelJS.Workbook();
  let sheet;
  if (fileExists(outFile)) {
    await workbook.xlsx.readFile(outFile);
    sheet = workbook.getWorksheet('Data') || workbook.addWorksheet('Data');
  } else {
    sheet = workbook.addWorksheet('Data');
    sheet.addRow([...customColumns, 'url', 'created_at']);
    sheet.getRow(1).font = { bold: true };
  }

  const profileDir = path.join(ROOT_DIR, 'profiles', 'universal_agent');
  ensureDir(profileDir);
  const context = await chromium.launchPersistentContext(profileDir, {
    headless: false,
    viewport: null
  });
  const page = context.pages()[0] || await context.newPage();
  
  if (targetUrl.trim()) {
      try {
          await page.goto(targetUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
      } catch (e) {
          console.log(chalk.red(`Không thể load URL: ${e.message}`));
      }
  }

  console.log(chalk.magenta('\n======================================================'));
  console.log(chalk.green('🚀 AUTO AI WEB AGENT ĐÃ KÍCH HOẠT!'));
  console.log(chalk.white('Trình duyệt rảnh tay: AI sẽ phân tích và điều khiển!'));
  console.log(chalk.magenta('======================================================\n'));

  let chatHistory = {
    contents: [
      {
        role: "user",
        parts: [{
          text: `Bạn là một Chuyên gia Cào dữ liệu (AI Web Scraper). Người dùng yêu cầu: "${taskPrompt}".
Các cột Excel cần trích xuất: ${customColumns.join(', ')}.

Bạn sẽ được cấp nội dung text của trang web và danh sách các phần tử tương tác (button, link, input) kèm theo ID [xyz].
Nhiệm vụ của bạn là suy nghĩ xem nên làm gì tiếp theo để đạt được mục tiêu của người dùng.

BẠN CHỈ ĐƯỢC PHÉP TRẢ VỀ ĐÚNG 1 STRING JSON HỢP LỆ (Không định dạng markdown xung quanh block), theo cấu trúc:
{
  "thought": "Suy luận của bạn (Ví dụ: Trang web có bộ lọc Quốc gia, tôi nên hỏi người dùng muốn lấy quốc gia nào)",
  "action": "ASK_USER" | "CLICK" | "TYPE" | "EXTRACT" | "WAIT" | "SCROLL_DOWN" | "SCROLL_UP",
  "element_id": 123, 
  "value": "Nếu hành động là TYPE, điền chữ muốn gõ. Nếu hành động là ASK_USER, điền câu hỏi để hỏi người dùng",
  "extracted_data": [ ... mảng JSON các object theo đúng tên cột, nếu action là EXTRACT ]
}
Lưu ý "element_id" phải là một số nguyên (number), nếu không có thì để null.

Nguyên tắc:
- Hãy sử dụng ASK_USER nếu bạn không chắc chắn người dùng muốn lọc theo tiêu chí nào (Quốc gia, Ngành học...), hoặc nếu cần người dùng vượt captcha/đăng nhập.
- BẠN CÓ "MẮT" LÀ ẢNH CHỤP MÀN HÌNH MÀ TÔI GỬI: Hãy quan sát ảnh để xem các Select box đã mở đúng chưa. Bạn là người điều khiển trang (dựa vào hình ảnh thực tế).
- Nếu danh sách chưa kéo tới cuối mạng, hoặc không tìm thấy thẻ "Next Page", hãy dùng SCROLL_DOWN để cuộn qua.
- Hãy dùng EXTRACT khi dữ liệu hiển thị tốt trên màn hình.
- Sau khi EXTRACT xong 1 trang, nếu có nút "Trang sau/Next Page", hãy CLICK để sang trang mới.`
        }]
      },
      {
        role: "model",
        parts: [{ text: `{"thought": "Đã hiểu nhiệm vụ, tôi đã sẵn sàng. Hãy cung cấp ảnh màn hình.", "action": "WAIT"}` }]
      }
    ]
  };

  while (true) {
    console.log(chalk.cyan(`\n[*] Agent đang phân tích trang web (${page.url()})...`));
    
    const domSnapshot = await page.evaluate(() => {
        let counter = 1;
        let interactables = [];
        
        document.querySelectorAll('a, button, input, select, [role="button"], [role="link"], [role="checkbox"]').forEach(el => {
            const rect = el.getBoundingClientRect();
            // Lọc ra các phần tử thực sự hiển thị trên màn hình hiện tại
            if (rect.width > 0 && rect.height > 0 && rect.top >= 0 && rect.top <= window.innerHeight + 500) {
                const text = (el.innerText || el.value || el.placeholder || el.getAttribute('aria-label') || '').replace(/\s+/g, ' ').trim();
                if (text || el.tagName.toLowerCase() === 'input') {
                    el.setAttribute('data-ai-id', counter);
                    interactables.push(`[${counter}] ${el.tagName}: ${text.substring(0, 40)}`);
                    counter++;
                }
            }
        });

        return {
           url: location.href,
           interactables: interactables.join('\n'),
           text: document.body.innerText.replace(/\s+/g, ' ').substring(0, 40000)
        };
    });

    const userPrompt = `--- TRẠNG THÁI HIỆN TẠI ---
URL: ${domSnapshot.url}

CÁC PHẦN TỬ TƯƠNG TÁC (Có ID):
${domSnapshot.interactables || '(Không có)'}

VĂN BẢN TRÊN TRANG (Trích xuất để lấy dữ liệu):
${domSnapshot.text}

Hãy phản hồi bằng đúng 1 object JSON chứa quyết định hành động tiếp theo của bạn.
(Gợi ý: Tôi có GỬI KÈM ẢNH CHỤP MÀN HÌNH (Screenshot) ở ngay dưới. Bạn có mắt, hãy nhìn ảnh để quyết định nhé!)`;

    const screenshotBuffer = await page.screenshot({ type: 'jpeg', quality: 30 }).catch(() => null);
    
    let partsArray = [{ text: userPrompt }];
    if (screenshotBuffer) {
        partsArray.push({
            inlineData: {
                mimeType: "image/jpeg",
                data: screenshotBuffer.toString('base64')
            }
        });
    }

    chatHistory.contents.push({ role: "user", parts: partsArray });

    // Giữ cho context window không quá dài để tránh lỗi Gemini Payload Too Large
    if (chatHistory.contents.length > 20) {
       chatHistory.contents.splice(2, 2); // Cắt bớt lịch sử giữa chừng, giữ lại prompt gốc
    }

    try {
      const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(GEMINI_MODEL)}:generateContent?key=${encodeURIComponent(GEMINI_API_KEY)}`;
      const response = await axios.post(url, {
        contents: chatHistory.contents,
        generationConfig: { temperature: 0.1 }
      }, { timeout: 120000, headers: { 'Content-Type': 'application/json' } });

      const replyText = response?.data?.candidates?.[0]?.content?.parts?.map(p => p.text || '').join('') || '';
      chatHistory.contents.push({ role: "model", parts: [{ text: replyText }] });

      let aiAction = {};
      try {
         const jsonMatch = replyText.match(/```json\n([\s\S]*?)\n```/) || replyText.match(/```\n([\s\S]*?)\n```/);
         if (jsonMatch) aiAction = JSON.parse(jsonMatch[1]);
         else {
             const cleaned = replyText.substring(replyText.indexOf('{'), replyText.lastIndexOf('}') + 1);
             aiAction = JSON.parse(cleaned);
         }
      } catch (e) {
         console.log(chalk.red(`[!] AI trả lời không chuẩn JSON. Cố gắng thử lại vòng lặp sau... (Lỗi Parse)`));
         chatHistory.contents.push({ role: "user", parts: [{ text: "Phản hồi trước đó không phải JSON hợp lệ. BẮT BUỘC trả về duy nhất 1 JSON object."}] });
         continue;
      }

      console.log(chalk.yellow(`\n[🧠 Agent Nghĩ]: ${aiAction.thought}`));

      if (aiAction.action === 'ASK_USER') {
         const answer = await ask(chalk.green(`[🤖 Agent Hỏi]: ${aiAction.value}\n=> (Bạn Gõ): `));
         chatHistory.contents.push({ role: "user", parts: [{ text: `Câu trả lời từ người dùng: ${answer}` }] });
         if (answer.toLowerCase() === 'exit') break;
      } 
      else if (aiAction.action === 'CLICK') {
         console.log(chalk.cyan(`[👉 Agent Click]: Click vào phần tử số [${aiAction.element_id}]`));
         const elHandle = await page.$(`[data-ai-id="${aiAction.element_id}"]`);
         if (elHandle) {
             await elHandle.scrollIntoViewIfNeeded().catch(()=>{});
             await elHandle.click({ force: true }).catch(err => console.log(chalk.red(`Lỗi click: ${err.message}`)));
             await sleep(2500); // Chờ hiệu ứng UI cập nhật
         } else {
             console.log(chalk.red(`[!] Không tìm thấy phần tử [${aiAction.element_id}] để click.`));
             chatHistory.contents.push({ role: "user", parts: [{ text: `Phần tử ID [${aiAction.element_id}] không tồn tại trên màn hình. Hãy chọn thao tác khác.`}] });
         }
      }
      else if (aiAction.action === 'TYPE') {
         console.log(chalk.cyan(`[⌨️ Agent Gõ]: Gõ "${aiAction.value}" vào phần tử số [${aiAction.element_id}]`));
         const elHandle = await page.$(`[data-ai-id="${aiAction.element_id}"]`);
         if (elHandle) {
             await elHandle.scrollIntoViewIfNeeded().catch(()=>{});
             await elHandle.fill(aiAction.value || '').catch(()=>{});
             await sleep(1000);
             await page.keyboard.press('Enter').catch(()=>{}); // Mô phỏng gõ Enter sau khi nhập
             await sleep(2000);
         } else {
             chatHistory.contents.push({ role: "user", parts: [{ text: `Phần tử ID [${aiAction.element_id}] không tồn tại trên màn hình.`}] });
         }
      }
      else if (aiAction.action === 'EXTRACT') {
         const data = aiAction.extracted_data || [];
         if (data.length > 0) {
            console.log(chalk.green(`[+] AI đã trích xuất thành công ${data.length} dòng dữ liệu!`));
            const now = DateTime.now().setZone(TIMEZONE).toISO();
            data.forEach(item => {
               const rowData = customColumns.map(c => escapeFormula(item[c] ?? ''));
               rowData.push(page.url(), now);
               sheet.addRow(rowData);
            });
            try {
               await workbook.xlsx.writeFile(outFile);
               console.log(chalk.green(`[+] Đã lưu vào ${outFile}`));
            } catch (err) {
               console.log(chalk.red(`[!] BẠN ĐANG MỞ FILE EXCEL NÊN KHÔNG THỂ LƯU! Hãy đóng file excel ngay. (${err.message})`));
            }
         } else {
            console.log(chalk.yellow(`[-] Lệnh EXTRACT nhưng mảng dữ liệu trả về rỗng.`));
         }

         const isContinue = await ask(chalk.green(`\nAgent vừa EXTRACT xong. Nhấn Enter để Agent TỰ ĐỘNG làm tiếp (Tìm nút Next Page), hoặc gõ 'exit' để dừng: `));
         if (isContinue.toLowerCase() === 'exit') break;
         chatHistory.contents.push({ role: "user", parts: [{ text: "Đã trích xuất và lưu xong. Hãy click sang tiếp trang sau hoặc thực hiện hành động tiếp theo." }] });
      }
      else if (aiAction.action === 'SCROLL_DOWN') {
         console.log(chalk.gray(`[⏬ Agent Cuộn]: Đang cuộn trang xuống...`));
         await page.mouse.wheel(0, 800).catch(()=>{});
         await sleep(2000);
      }
      else if (aiAction.action === 'SCROLL_UP') {
         console.log(chalk.gray(`[⏫ Agent Cuộn]: Đang cuộn trang lên...`));
         await page.mouse.wheel(0, -800).catch(()=>{});
         await sleep(2000);
      }
      else if (aiAction.action === 'WAIT') {
         console.log(chalk.gray(`[⏳ Agent Chờ]: Đang đợi trang web load/phản hồi...`));
         await sleep(3000);
      }
      else {
         console.log(chalk.red(`[!] Agent Không Hiểu Lệnh: Hành động không hợp lệ -> ${aiAction.action}`));
         chatHistory.contents.push({ role: "user", parts: [{ text: `Hành động ${aiAction.action} không hợp lệ. Chỉ chọn ASK_USER, CLICK, TYPE, EXTRACT, hoặc WAIT.` }] });
         await sleep(2000);
      }

    } catch (error) {
       console.log(chalk.red(`[-] Lỗi hệ thống khi gọi AI: ${error.message}`));
       await sleep(3000);
    }
  }

  await context.close();
  console.log(chalk.green('Đã đóng Agent Crawler.'));
}

async function main() {
  banner();
  ensureDir(path.join(ROOT_DIR, 'state'));
  ensureDir(path.dirname(OUTPUT_EXCEL));

  console.log(chalk.magenta('\n================ CHỌN MODULE HOẠT ĐỘNG ================'));
  console.log(chalk.cyan('  1. AI Web Agent: Cào dữ liệu từ trang web bất kỳ (MỚI)'));
  console.log(chalk.cyan('  2. Gmail AI Bot: Đồng bộ & xử lý hộp thư Gmail (Mặc định)'));
  console.log(chalk.magenta('======================================================='));
  const modeInput = await ask('\nNhập lựa chọn của bạn (1 hoặc 2. Nhấn Enter để chọn 2): ');

  if (modeInput.trim() === '1') {
    console.log(chalk.yellow('\n--- THIẾT LẬP AI WEB AGENT ---'));
    let url = await ask('1. Nhập URL trang web muốn cào (Ví dụ: https://unisetu.com): ');
    let task = await ask('2. Bạn muốn AI trích xuất gì? (Ví dụ: Lọc tên khóa học, học phí...): ');
    let cols = await ask('3. Tên cột xuất Excel (Ví dụ: TenKhoaHoc, HocPhi, ThoiGian): ');
    
    if (url && !url.startsWith('http')) url = 'https://' + url;

    await runUniversalAgent(url, task, cols);
    return;
  }

  let accounts = loadAccounts();

  if (accounts.length === 0) {
    console.log(chalk.yellow('\nChưa có tài khoản nào được cấu hình trong accounts.json.'));
    const addInput = await ask('Bạn có muốn thêm cấu hình và đăng nhập tài khoản Gmail mới để bắt đầu không? (y/n): ');
    if (addInput.toLowerCase() === 'y') {
       accounts = await handleAddAccountFlow(accounts);
    } else {
       console.log(chalk.red('Không thể tiếp tục mà không có tài khoản.'));
       process.exit(1);
    }
  } else {
    console.log(chalk.cyan('\n--- TÀI KHOẢN HIỆN TẠI ---'));
    accounts.forEach((acc, i) => {
       console.log(chalk.cyan(`  ${i+1}. ${acc.email} (Trạng thái: ${acc.enabled ? 'Bật' : 'Tắt'})`));
    });
    
    const addMore = await ask('\nBạn có muốn thêm tài khoản Gmail mới không? (y/n - hoặc bỏ trống để bỏ qua): ');
    if (addMore.toLowerCase() === 'y') {
       accounts = await handleAddAccountFlow(accounts);
    }
  }

  let selectedAccounts = [];
  const activeAccounts = accounts.filter(a => a.enabled);
  
  if (activeAccounts.length === 0) {
    console.log(chalk.red('Không có tài khoản nào (đang Bật) để chạy. Vui lòng kiểm tra accounts.json.'));
    process.exit(1);
  } else if (activeAccounts.length === 1) {
    selectedAccounts = activeAccounts;
  } else {
    const selection = await ask(`\nBạn muốn cào mail từ những tài khoản nào? (Gõ 1, 2... cách nhau dấu phẩy, hoặc 'all' để lấy tất cả): `);
    if (selection.trim().toLowerCase() === 'all' || selection.trim() === '') {
      selectedAccounts = activeAccounts;
    } else {
      const parts = selection.split(',').map(s => parseInt(s.trim())).filter(n => !isNaN(n));
      if (parts.length > 0) {
         selectedAccounts = activeAccounts.filter((_, idx) => parts.includes(idx + 1));
      }
      if (selectedAccounts.length === 0) {
         console.log(chalk.yellow('Lựa chọn không hợp lệ, sẽ tự động lấy tất cả các tài khoản.'));
         selectedAccounts = activeAccounts;
      }
    }
  }

  if (process.argv.includes('--setup')) {
    await setupMode(selectedAccounts);
    return;
  }

  while (true) {
    // --- HIỆN TRẠNG THÁI ---
    console.log(chalk.yellow('\n--- KIỂM TRA DỮ LIỆU ĐÃ CÓ ---'));
    for (const acc of selectedAccounts) {
      const stateFile = path.join(ROOT_DIR, 'state', `${acc.id}_processed.json`);
      const state = safeJsonRead(stateFile, { keys: {} });
      const count = Object.keys(state.keys).length;
      console.log(chalk.gray(`[${acc.email}] Đã xử lý: ${count} mail.`));
    }

    // --- HỎI NGƯỜI DÙNG QUY TRÌNH ---
    console.log(chalk.yellow('\n--- THIẾT LẬP AI ---'));
    let customPrompt = await ask(`Bạn muốn AI lọc thông tin gì? (Ví dụ: 'tập trung lấy mã OTP', để trống = chạy mặc định): `);
    let customColumns = [];

    EXCEL_HEADERS = [
      'account_id', 'email', 'sender_email', 'subject',
      'received_at_raw', 'received_at_iso', 'received_at_local', 'body_text', 'ai_json', 'ai_status',
      'ai_confidence', 'ai_error', 'request_type', 'customer_name', 'phone', 'email_extracted', 'order_code',
      'amount', 'currency', 'note', 'created_at'
    ];

    console.log(chalk.yellow('\n--- THIẾT LẬP PHIÊN CHẠY ---'));
    const targetCountInput = await ask(`Bạn muốn lấy thêm bao nhiêu mail mỗi tài khoản? (Nhập số, hoặc 'all', hoặc 'exit' để thoát): `);
    
    if (targetCountInput.toLowerCase() === 'exit') {
      console.log(chalk.green('Thoát chương trình.'));
      break;
    }
    
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
            forceProcess,
            customPrompt,
            customColumns
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

    console.log(chalk.green('Hoàn tất toàn bộ tiến trình vòng này. Chuẩn bị lặp lại...'));
  }
}

if (isMainThread) {
  const runner = process.argv.includes('--telegram-run') ? runTelegramMode : main;
  runner().catch(error => {
    if (TELEGRAM_MODE) {
      emitTelegramEvent('error', cleanText(error.message || String(error)));
      process.exit(1);
      return;
    }
    console.error(chalk.red(`Lỗi rồi: ${error.message}`));
    process.exit(1);
  });
} else {
  runWorker(workerData);
}
