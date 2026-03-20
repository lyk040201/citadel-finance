
import os
import requests
import pdfplumber
import datetime
import math
import time
import base64
import json
import urllib3
import matplotlib.pyplot as plt
import matplotlib
import streamlit as st
from zhipuai import ZhipuAI
from tavily import TavilyClient

# 屏蔽 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 设置中文字体
matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'Microsoft YaHei']
matplotlib.rcParams['axes.unicode_minus'] = False 

# ==========================================
# 1. 核心配置区
# ==========================================
WORKSPACE_DIR = "workspace"

if not os.path.exists(WORKSPACE_DIR):
    os.makedirs(WORKSPACE_DIR)

# === 智能感知核心 ===
FINANCIAL_STOCKS_BANKS = {"601988": "中国银行", "601398": "工商银行", "601288": "农业银行", "601939": "建设银行", "000001": "平安银行", "600036": "招商银行"}

def get_industry_type(stock_code):
    clean_code = ''.join(filter(str.isdigit, stock_code))
    if clean_code in FINANCIAL_STOCKS_BANKS: return "BANK"
    return "GENERAL"

# === Session State ===
if "final_report" not in st.session_state:
    st.session_state.final_report = None
    st.session_state.archived_pdfs = []
    st.session_state.figs = []
    st.session_state.super_financial_base = ""
if "guessed_code" not in st.session_state:
    st.session_state.guessed_code = ""

# ==========================================
# 2. 强行破壁 PDF 下载器
# ==========================================
def generate_mcode(): return base64.b64encode(str(math.floor(time.time())).encode('utf-8')).decode('utf-8')

def download_official_pdf(stock_code, target_year, company_name):
    stock_code = ''.join(filter(str.isdigit, stock_code)).zfill(6)
    local_filename = f"{stock_code}_{target_year}年_官方原件.pdf"
    pdf_path = os.path.join(WORKSPACE_DIR, local_filename)

    pdf_url = None; art_code = None; found_by_em = False; msg_log = []
    session = requests.Session(); session.trust_env = False
    
    # 【代理开关】: 如果跨境访问报错，去掉下面这行的注释并修改端口
    # session.proxies = {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}

    # 引擎 A: 巨潮资讯网
    try:
        cninfo_headers = {"User-Agent": "Mozilla/5.0", "mcode": generate_mcode(), "Origin": "http://www.cninfo.com.cn", "Referer": "http://www.cninfo.com.cn/new/index"}
        search_res = session.post("http://www.cninfo.com.cn/new/information/topSearch/query", data={"keyWord": stock_code}, headers=cninfo_headers, timeout=15)
        search_data = search_res.json()
        if search_data and isinstance(search_data, list):
            org_id = search_data[0].get('orgId'); full_code = search_data[0].get('code')
            query_params = {"pageNum": 1, "pageSize": 30, "column": "szse", "tabName": "fulltext", "stock": f"{full_code},{org_id}", "category": "category_ndbg_szsh", "seDate": f"{int(target_year)}-01-01~{int(target_year)+2}-06-30"}
            query_res = session.post("http://www.cninfo.com.cn/new/hisAnnouncement/query", data=query_params, headers=cninfo_headers, timeout=15)
            for ann in query_res.json().get('announcements', []):
                title = ann['announcementTitle']
                if "摘要" not in title and "英文" not in title and str(target_year) in title and "年度报告" in title:
                    pdf_url = "http://static.cninfo.com.cn/" + ann['adjunctUrl']
                    msg_log.append("✅ [寻址系统] 巨潮网节点锁定。"); break
    except Exception: msg_log.append("⚠️ 巨潮网拒绝响应，切换东方财富...")

    # 引擎 B: 东方财富
    if not pdf_url:
        try:
            prefix = "SH" if stock_code.startswith("6") else "BJ" if stock_code.startswith(("8", "4")) else "SZ"
            stock_list = f"{prefix}{stock_code}"
            em_headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://data.eastmoney.com/"}
            for page_idx in range(1, 4):
                em_params = {"page_size": 100, "page_index": page_idx, "ann_type": "A", "client_source": "web", "stock_list": stock_list}
                try: em_res = session.get("https://np-anotice-stock.eastmoney.com/api/security/ann", params=em_params, headers=em_headers, timeout=15, verify=False)
                except requests.exceptions.SSLError: em_res = session.get("http://np-anotice-stock.eastmoney.com/api/security/ann", params=em_params, headers=em_headers, timeout=15)
                data_list = em_res.json().get('data', {}).get('list', [])
                if not data_list: break
                for ann in data_list:
                    title = ann['title']; sec_name = ann.get('sec_name', '')
                    if company_name[:2] not in title and company_name[:2] not in sec_name: continue 
                    if "摘要" not in title and "英文" not in title and str(target_year) in title and "年度报告" in title:
                        art_code = ann['art_code']; found_by_em = True
                        msg_log.append(f"✅ [寻址系统] 东方财富节点锁定。"); break
                if found_by_em: break
        except Exception as e: msg_log.append(f"❌ 容灾节点失败: {e}")

    # 物理抓取
    urls_to_try = [pdf_url] if pdf_url else [f"https://pdf.dfcfw.com/pdf/H2_{art_code}_1.pdf"] if found_by_em else []
    if not urls_to_try: return None, "\n".join(msg_log) + "\n❌ 致命错误：全网无法匹配到该财报下载地址。"

    stealth_headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/pdf,*/*", "Referer": "https://data.eastmoney.com/"}
    for url in urls_to_try:
        try:
            pdf_res = session.get(url, headers=stealth_headers, stream=True, timeout=60, verify=False)
            if pdf_res.status_code == 200 and 'text/html' not in pdf_res.headers.get('Content-Type', '').lower():
                with open(pdf_path, "wb") as f:
                    for chunk in pdf_res.iter_content(chunk_size=1024):
                        if chunk: f.write(chunk)
                if os.path.getsize(pdf_path) >= 100000:
                    msg_log.append(f"✅ 强行抓取成功 ({os.path.getsize(pdf_path)//1024} KB)。")
                    return pdf_path, "\n".join(msg_log)
                else: os.remove(pdf_path); msg_log.append(f"⚠️ 拦截警报：下载到空壳文件已被销毁。")
        except Exception as e:
            if os.path.exists(pdf_path): os.remove(pdf_path)
    return None, "\n".join(msg_log) + "\n❌ 强行抓取失败！跨境IP被阻断，请开启 proxies 代理或手动放置文件。"

# ==========================================
# 3. 雷达切除 & 动态 API 调用
# ==========================================
def extract_core_financial_statements(pdf_path, year, industry_type):
    statements_text = []
    target_keywords = ["合并资产负债表", "合并利润表", "合并现金流量表", "合并所有者权益变动表"]
    if industry_type == "BANK":
        target_keywords = ["资产负债表", "利润表", "现金流量表", "不良贷款", "资本充足率", "拨备覆盖率", "净息差"]
    pages_extracted = 0
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for i in range(len(pdf.pages)):
                page = pdf.pages[i]
                text = page.extract_text()
                if not text: continue
                if any(kw in text for kw in target_keywords):
                    tables = page.extract_tables()
                    if tables:
                        statements_text.append(f"\n--- 📄 {year}年度原件 第 {i+1} 页 ---")
                        for table in tables:
                            for row in table:
                                clean_row = [str(c).replace('\n', '') if c else '-' for c in row]
                                statements_text.append("| " + " | ".join(clean_row) + " |")
                        pages_extracted += 1
                if pages_extracted >= 10: break
        return "\n".join(statements_text)
    except Exception as e: return f"❌ {year}年解析异常: {e}"

def ask_zhipu_grounded(prompt, system_role):
    if not st.session_state.zhipu_key:
        return "❌ 缺少 ZhipuAI API Key，请先在侧边栏配置。"
    try:
        client = ZhipuAI(api_key=st.session_state.zhipu_key)
        response = client.chat.completions.create(model="glm-4", messages=[{"role": "system", "content": system_role}, {"role": "user", "content": prompt[:20000]}], top_p=0.7, temperature=0.1)
        return response.choices[0].message.content
    except Exception as e: return f"API 接入异常: {e}"

# ==========================================
# 4. Streamlit 图表生成器
# ==========================================
def generate_visual_charts_v2(super_financial_base, company_name, industry_type):
    json_prompt = ""
    if industry_type == "GENERAL":
        json_prompt = f"""请从以下底稿提取过去三年的核心数据。严格返回 JSON 代码。
        格式要求：
        {{
            "years": ["2022", "2023", "2024"],
            "revenue": [100.5, 120.3, 150.8],
            "profit": [10.2, 12.5, 15.1],
            "operating_cash_flow": [12.0, 8.5, 18.2],
            "gross_margin": [25.5, 24.0, 26.1],
            "roe": [15.2, 16.5, 14.8],
            "eps": [1.2, 1.5, 1.4],
            "latest_assets": 500.0,
            "latest_liabilities": 200.0
        }}
        """
    elif industry_type == "BANK":
        json_prompt = f"""请从以下底稿提取过去三年的核心数据。严格返回 JSON 代码。
        格式要求：
        {{
            "years": ["2022", "2023", "2024"],
            "revenue": [100.5, 120.3, 150.8],
            "profit": [10.2, 12.5, 15.1],
            "net_interest_margin": [1.9, 1.85, 1.75],
            "npl_ratio": [1.4, 1.35, 1.3],
            "provision_coverage": [180.5, 190.2, 195.1],
            "roe": [10.2, 10.5, 10.1],
            "eps": [0.8, 0.85, 0.82],
            "capital_adequacy": [12.5, 13.0, 13.5]
        }}
        """
    
    full_prompt = json_prompt + f"\n底稿：\n{super_financial_base[:15000]}"
    json_res = ask_zhipu_grounded(full_prompt, "你是一个只输出严格 JSON 的机器。")
    
    figs = []
    try:
        clean_json = json_res.replace('```json', '').replace('```', '').strip()
        data = json.loads(clean_json)
        
        if industry_type == "GENERAL":
            fig1, ax1 = plt.subplots(figsize=(6, 4)); ax1.plot(data['years'], data['revenue'], marker='o', label='营业总收入 (亿元)'); ax1.plot(data['years'], data['profit'], marker='s', label='归母净利润 (亿元)'); ax1.set_title("业绩体量趋势", fontweight='bold'); ax1.legend(); ax1.grid(True, linestyle='--', alpha=0.6); figs.append(("体量趋势", fig1))
            fig2, ax2 = plt.subplots(figsize=(6, 4)); x = range(len(data['years'])); width = 0.35; ax2.bar([i - width/2 for i in x], data['profit'], width, label='净利润'); ax2.bar([i + width/2 for i in x], data['operating_cash_flow'], width, label='经营现金流'); ax2.set_xticks(x); ax2.set_xticklabels(data['years']); ax2.set_title("盈余质量检验 (造假排雷)", fontweight='bold'); ax2.legend(); ax2.grid(axis='y', linestyle='--', alpha=0.6); figs.append(("盈余质量", fig2))
            fig3, ax3 = plt.subplots(figsize=(6, 4)); ax3.plot(data['years'], data['gross_margin'], marker='^', label='毛利率 (%)', color='#d62728'); ax3.set_title("护城河/盈利率趋势", fontweight='bold'); ax3.legend(); ax3.grid(True, linestyle='--', alpha=0.6); figs.append(("盈利能力", fig3))
            fig4, ax4 = plt.subplots(figsize=(6, 4)); net_assets = data['latest_assets'] - data['latest_liabilities']; ax4.pie([data['latest_liabilities'], max(net_assets, 0)], labels=['总负债', '净资产'], autopct='%1.1f%%', shadow=True, startangle=140); ax4.set_title("最新一期杠杆结构", fontweight='bold'); figs.append(("资本结构", fig4))
            
        elif industry_type == "BANK":
            fig1, ax1 = plt.subplots(figsize=(6, 4)); ax1.plot(data['years'], data['revenue'], marker='o', label='营业收入 (亿元)'); ax1.plot(data['years'], data['profit'], marker='s', label='归母净利润 (亿元)'); ax1.set_title("核心业绩体量", fontweight='bold'); ax1.legend(); ax1.grid(True, linestyle='--', alpha=0.6); figs.append(("核心业绩", fig1))
            fig2, ax2 = plt.subplots(figsize=(6, 4)); x = range(len(data['years'])); width = 0.35; ax2.bar(x, data['npl_ratio'], width, label='不良贷款率 NPL (%)', color='#ff7f0e', alpha=0.8); ax2.set_ylabel('NPL (%)', color='#ff7f0e'); ax2.set_xticks(x); ax2.set_xticklabels(data['years']); ax2_r = ax2.twinx(); ax2_r.plot(x, data['provision_coverage'], marker='^', label='拨备覆盖率 PCR (%)', color='#2ca02c'); ax2_r.set_ylabel('PCR (%)', color='#2ca02c'); ax2.set_title("资产质量安全盾 (防线)", fontweight='bold'); ax2.grid(axis='y', linestyle='--', alpha=0.6); figs.append(("资产质量", fig2))
            fig3, ax3 = plt.subplots(figsize=(6, 4)); ax3.plot(data['years'], data['net_interest_margin'], marker='^', label='净息差 NIM (%)', color='#d62728'); ax3.set_title("护城河/盈利率 (NIM)", fontweight='bold'); ax3.legend(); ax3.grid(True, linestyle='--', alpha=0.6); figs.append(("盈利能力", fig3))
            fig4, ax4 = plt.subplots(figsize=(6, 4)); ax4.fill_between(data['years'], data['capital_adequacy'], label='核心一级资本充足率 (%)', color='#66b3ff', alpha=0.6); ax4.plot(data['years'], data['capital_adequacy'], color='#1f77b4', marker='o'); ax4.set_title("风险控制体系 (CAR)", fontweight='bold'); ax4.legend(); ax4.grid(True, linestyle='--', alpha=0.6); figs.append(("风险控制", fig4))

        fig5, ax5 = plt.subplots(figsize=(6, 4))
        ax5.plot(data['years'], data['roe'], marker='D', label='净资产收益率 ROE (%)', color='#9467bd', linewidth=2)
        ax5.set_title(f"股东真实回报天花板 (ROE)", fontweight='bold'); ax5.set_ylabel('%'); ax5.legend(); ax5.grid(True, linestyle='--', alpha=0.6)
        figs.append(("股东回报", fig5))
        
        fig6, ax6 = plt.subplots(figsize=(6, 4))
        ax6.bar(data['years'], data['eps'], label='基本每股收益 EPS (元/股)', color='#17becf', alpha=0.8)
        for i, v in enumerate(data['eps']): ax6.text(i, v + 0.05, str(v), ha='center', va='bottom', fontsize=9)
        ax6.set_title(f"单股含金量绝对值 (EPS)", fontweight='bold'); ax6.set_ylabel('元'); ax6.legend(); ax6.grid(axis='y', linestyle='--', alpha=0.6)
        figs.append(("每股收益", fig6))

        return figs
    except Exception as e: return []

def display_pdf_preview(file_path):
    try:
        with pdfplumber.open(file_path) as pdf:
            img = pdf.pages[0].to_image(resolution=150).original
            st.image(img, caption=f"📄 {os.path.basename(file_path)} (原件首页)", use_container_width=True)
            st.info("💡 系统提取带公章的首页供初步验证。下载查看完整原件。")
    except: st.error(f"预览生成失败。")

# ==========================================
# 5. Streamlit 主界面 (App UI)
# ==========================================
st.set_page_config(page_title=" Citadel 全行业穿透投研系统", page_icon="🏦", layout="wide")

st.title("🏦 A股机构级全行业官方原件穿透投研系统 (Citadel)")
st.markdown("---")

with st.sidebar:
    st.header("🔑 平台 API 凭证配置")
    st.info("为保障数据隐私，平台不保存任何 API Key，请在此次会话中输入您的专属密钥。")
    st.session_state.zhipu_key = st.text_input("ZhipuAI API Key (必填)", type="password", help="获取地址: open.bigmodel.cn")
    st.session_state.tavily_key = st.text_input("Tavily API Key (可选)", type="password", help="用于网络实时风控查询")
    
    st.markdown("---")
    st.header("⚙️ 投研控制台")
    st.info("🎯 A 股全行业专精模式。包含六芒星全维指标看板。")
    
    company_name = st.text_input("输入A股企业名称 (如: 比亚迪、中国银行)", value="比亚迪", key="comp_name")
    
    def guess_code_action():
        if not st.session_state.zhipu_key:
            st.error("请先在上方配置 ZhipuAI API Key")
            return
        if st.session_state.comp_name:
            prompt = f"请查询【{st.session_state.comp_name}】在A股（沪深北交易所）的股票代码。只回复6位纯数字代码。不要其他文字。"
            res = ask_zhipu_grounded(prompt, "你是一个只输出纯数字的金融机器。")
            st.session_state.guessed_code = ''.join(filter(str.isdigit, res))[:6]
            
    st.button("🔍 AI智能匹配 A 股代码", on_click=guess_code_action, use_container_width=True)
    stock_code = st.text_input("✏️ 确认/修改 股票代码 (6位数字)", value=st.session_state.guessed_code)
    
    st.markdown("---")
    base_year_str = st.selectbox("基准年份", ["2024", "2023", "2022"])
    
    col_run, col_stop = st.columns(2)
    with col_run: start_btn = st.button("🚀 启动穿透审计", type="primary", use_container_width=True)
    with col_stop: stop_btn = st.button("🛑 取消", type="secondary", use_container_width=True)
    
    if stop_btn: st.warning("紧急终止。"); st.stop()

if start_btn:
    if not st.session_state.zhipu_key:
        st.error("🚨 请先在左侧输入大模型 API Key！")
        st.stop()
    if not company_name or not stock_code or len(stock_code) != 6:
        st.warning("🚨 确保代码为严格的 A 股 6 位数字！")
    else:
        st.session_state.final_report = None; st.session_state.archived_pdfs = []; st.session_state.figs = []; st.session_state.super_financial_base = ""
        base_year = int(base_year_str)
        years_to_audit = [base_year, base_year - 1, base_year - 2]
        
        industry_type = get_industry_type(stock_code)
        
        multi_year_data = {}
        with st.status(f"正在对 {company_name}({stock_code}) 执行智能穿透...", expanded=True) as status:
            if industry_type == "BANK": st.warning(f"🏦 激活【大金融排雷引擎】模式。")
            else: st.success(f"🏭 激活【实体企业审计引擎】模式。")
                
            for year in years_to_audit:
                pdf_path, msg = download_official_pdf(stock_code, str(year), company_name)
                st.write(msg)
                if pdf_path:
                    st.session_state.archived_pdfs.append(pdf_path)
                    st.write(f"🔬 正在雷达扫描 {year}年 档案...")
                    multi_year_data[str(year)] = extract_core_financial_statements(pdf_path, year, industry_type)
                else: st.error(f"⚠️ {year}年 获取失败。")

            if not multi_year_data:
                status.update(label="🚨 熔断：公网物理拦截且本地无合法原件。请检查跨境IP节点或尝试手工空投。", state="error")
                st.stop()

            st.session_state.super_financial_base = "\n\n".join([f"=== {y}年底稿 ===\n{txt}" for y, txt in multi_year_data.items()])
            
            # 使用动态的 Tavily Key
            if st.session_state.tavily_key:
                try: 
                    tavily = TavilyClient(api_key=st.session_state.tavily_key)
                    news_data = tavily.search(query=f"{company_name} 最新 风险 评价", max_results=3)
                    news_data = "\n".join([f"标题: {r['title']}" for r in news_data['results']])
                except: news_data = "收集情报失败，请检查 API 状态。"
            else:
                news_data = "用户未配置 Tavily API，跳过风控资讯搜索。"

            st.write("🧠 基于雷达切除底稿执行严苛 AI 审计...")
            
            role_hint = "严苛精算师" if industry_type == "GENERAL" else "首席银行排雷专家"
            prompt_hint = "財務雷點和 ROE/EPS 回报能力" if industry_type == "GENERAL" else "不良貸款、資本充足率和 ROE 盈利能力"
            audit_res = ask_zhipu_grounded(f"严格基于以下三年物理底稿数据，深度审计 {company_name} 的趋势、{prompt_hint}：\n{st.session_state.super_financial_base}", f"你是{role_hint}。")
            
            st.session_state.final_report = ask_zhipu_grounded(f"三年精算审计报告：\n{audit_res}\n当前现状：\n{news_data}", f"你是CIO。撰写# {company_name} 全行业复盘报告。给出全行业的买卖建议。报告最后强制錨定官方底稿出处。")
            
            st.session_state.figs = generate_visual_charts_v2(st.session_state.super_financial_base, company_name, industry_type)
            
            status.update(label="🎉 穿透审计完成！请全副武装查阅您的 CIO 战报。", state="complete", expanded=False)

# ==========================================
# 结果展示区 (六芒星动态排版)
# ==========================================
if st.session_state.final_report:
    tab1, tab2, tab3, tab4 = st.tabs(["📝 CIO 深度战报", "📊 六芒星数据画布", "📕 档案库封皮验证", "🗄️ 弹药追溯底稿"])
    
    with tab1: st.markdown(st.session_state.final_report)
    with tab2:
        if st.session_state.figs:
            industry_type = get_industry_type(st.session_state.guessed_code)
            看板名称 = "【大金融专属 + 通用回报】" if industry_type == "BANK" else "【实体基本盘 + 通用回报】"
            st.success(f"💡 {看板名称} 所有数字由 AI 锚定官方 PDF 强行榨取，涵盖专业防线与通用股东回报 (ROE/EPS)。")
            
            col1, col2 = st.columns(2)
            if len(st.session_state.figs) > 0: col1.pyplot(st.session_state.figs[0][1])
            if len(st.session_state.figs) > 1: col2.pyplot(st.session_state.figs[1][1])
            
            col3, col4 = st.columns(2)
            if len(st.session_state.figs) > 2: col3.pyplot(st.session_state.figs[2][1])
            if len(st.session_state.figs) > 3: col4.pyplot(st.session_state.figs[3][1])
            
            st.markdown("---")
            st.subheader("🌟 跨行业通用价值标尺 (股东回报)")
            col5, col6 = st.columns(2)
            if len(st.session_state.figs) > 4: col5.pyplot(st.session_state.figs[4][1])
            if len(st.session_state.figs) > 5: col6.pyplot(st.session_state.figs[5][1])
            
        else: st.info("看板数据缺失。")
    with tab3:
        if st.session_state.archived_pdfs:
            selected_pdf = st.selectbox("选择年份封皮:", st.session_state.archived_pdfs)
            display_pdf_preview(selected_pdf)
            with open(selected_pdf, "rb") as file: st.download_button(label="📥 调阅完整原件 PDF", data=file, file_name=os.path.basename(selected_pdf), mime="application/pdf")
    with tab4: st.markdown("从 PDF 中强行切除出的纯文本数字底稿："); st.text_area("三年财务底稿明细", st.session_state.super_financial_base, height=500)
