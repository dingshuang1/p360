"""
A股数据获取工具
使用 akshare 库获取 A股实时行情数据
"""
import akshare as ak
from langchain.tools import tool
from langchain.tools import ToolRuntime
from coze_coding_utils.runtime_ctx.context import new_context
import json


@tool
def get_stock_index_data(runtime: ToolRuntime = None) -> str:
    """
    获取 A股主要指数数据（上证指数、深证成指、创业板指、科创50）
    
    Returns:
        str: JSON 格式的指数数据，包含代码、名称、最新价、涨跌幅等
    """
    try:
        ctx = runtime.context if runtime else new_context(method="get_stock_index")
        
        # 获取所有A股实时行情（包括指数）
        df = ak.stock_zh_a_spot_em()
        
        # 筛选主要指数
        indices = {
            "上证指数": "000001",
            "深证成指": "399001", 
            "创业板指": "399006",
            "科创50": "000688"
        }
        
        result = {}
        for name, code in indices.items():
            # 查找对应的指数数据
            index_data = df[df['代码'] == code]
            if not index_data.empty:
                idx = index_data.iloc[0]
                result[name] = {
                    "代码": code,
                    "最新价": float(idx.get('最新价', 0)),
                    "涨跌幅": float(idx.get('涨跌幅', 0)),
                    "涨跌额": float(idx.get('涨跌额', 0)),
                    "成交量": float(idx.get('成交量', 0)),
                    "成交额": float(idx.get('成交额', 0))
                }
            else:
                result[name] = {
                    "代码": code,
                    "最新价": 0,
                    "涨跌幅": 0,
                    "涨跌额": 0,
                    "成交量": 0,
                    "成交额": 0,
                    "note": "暂无数据"
                }
        
        return json.dumps(result, ensure_ascii=False, indent=2)
        
    except Exception as e:
        # 返回模拟数据以便演示功能
        return json.dumps({
            "上证指数": {
                "代码": "000001",
                "最新价": 3085.15,
                "涨跌幅": 0.52,
                "涨跌额": 15.93,
                "成交量": 125680000,
                "成交额": 158965000000,
                "note": "演示数据"
            },
            "深证成指": {
                "代码": "399001",
                "最新价": 9856.78,
                "涨跌幅": 0.78,
                "涨跌额": 76.23,
                "成交量": 234567000,
                "成交额": 345678000000,
                "note": "演示数据"
            },
            "创业板指": {
                "代码": "399006",
                "最新价": 1956.89,
                "涨跌幅": -0.23,
                "涨跌额": -4.51,
                "成交量": 98765400,
                "成交额": 123456000000,
                "note": "演示数据"
            },
            "科创50": {
                "代码": "000688",
                "最新价": 892.45,
                "涨跌幅": 1.25,
                "涨跌额": 11.02,
                "成交量": 45678900,
                "成交额": 67890000000,
                "note": "演示数据"
            },
            "error": str(e)
        }, ensure_ascii=False, indent=2)


@tool
def get_stock_ranking(runtime: ToolRuntime = None) -> str:
    """
    获取 A股涨跌幅排行榜（涨幅榜和跌幅榜）
    
    Returns:
        str: JSON 格式的涨跌幅排行榜数据
    """
    try:
        ctx = runtime.context if runtime else new_context(method="get_stock_ranking")
        
        # 获取所有A股数据
        df = ak.stock_zh_a_spot_em()
        
        # 获取今日涨幅榜（前20名）
        df_rise = df.sort_values('涨跌幅', ascending=False).head(20)
        
        # 获取今日跌幅榜（前20名）
        df_fall = df.sort_values('涨跌幅', ascending=True).head(20)
        
        # 提取需要的列
        columns = ['代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额']
        
        result = {
            "涨幅榜": [],
            "跌幅榜": []
        }
        
        for _, row in df_rise[columns].iterrows():
            result["涨幅榜"].append({
                "代码": row['代码'],
                "名称": row['名称'],
                "最新价": float(row['最新价']),
                "涨跌幅": float(row['涨跌幅']),
                "涨跌额": float(row['涨跌额']),
                "成交量": float(row['成交量']),
                "成交额": float(row['成交额'])
            })
        
        for _, row in df_fall[columns].iterrows():
            result["跌幅榜"].append({
                "代码": row['代码'],
                "名称": row['名称'],
                "最新价": float(row['最新价']),
                "涨跌幅": float(row['涨跌幅']),
                "涨跌额": float(row['涨跌额']),
                "成交量": float(row['成交量']),
                "成交额": float(row['成交额'])
            })
        
        return json.dumps(result, ensure_ascii=False, indent=2)
        
    except Exception as e:
        # 返回模拟数据以便演示功能
        return json.dumps({
            "涨幅榜": [
                {"代码": "600123", "名称": "兰花科创", "最新价": 15.68, "涨跌幅": 10.05, "涨跌额": 1.43, "成交量": 125680000, "成交额": 1589650000},
                {"代码": "002456", "名称": "欧菲光", "最新价": 12.35, "涨跌幅": 9.98, "涨跌额": 1.12, "成交量": 234567000, "成交额": 3456780000},
            ],
            "跌幅榜": [
                {"代码": "300456", "名称": "赛微电子", "最新价": 23.45, "涨跌幅": -9.95, "涨跌额": -2.59, "成交量": 98765400, "成交额": 1234560000},
                {"代码": "600789", "名称": "鲁抗医药", "最新价": 8.92, "涨跌幅": -9.92, "涨跌额": -0.98, "成交量": 45678900, "成交额": 678900000},
            ],
            "note": "演示数据",
            "error": str(e)
        }, ensure_ascii=False, indent=2)


@tool
def get_market_statistics(runtime: ToolRuntime = None) -> str:
    """
    获取市场统计数据（涨跌停统计、上涨下跌股票数）
    
    Returns:
        str: JSON 格式的市场统计数据
    """
    try:
        ctx = runtime.context if runtime else new_context(method="get_market_statistics")
        
        # 获取所有A股数据
        df = ak.stock_zh_a_spot_em()
        
        # 统计涨跌停
        limit_up = len(df[df['涨跌幅'] >= 9.9])  # 涨停
        limit_down = len(df[df['涨跌幅'] <= -9.9])  # 跌停
        
        # 统计涨跌家数
        up_count = len(df[df['涨跌幅'] > 0])
        down_count = len(df[df['涨跌幅'] < 0])
        flat_count = len(df[df['涨跌幅'] == 0])
        
        # 计算平均涨跌幅
        avg_change = df['涨跌幅'].mean()
        
        # 计算总成交额
        total_amount = df['成交额'].sum()
        
        result = {
            "涨跌停统计": {
                "涨停家数": limit_up,
                "跌停家数": limit_down
            },
            "涨跌家数": {
                "上涨家数": up_count,
                "下跌家数": down_count,
                "平盘家数": flat_count
            },
            "市场表现": {
                "平均涨跌幅": round(avg_change, 2),
                "总成交额": round(total_amount / 100000000, 2)  # 转换为亿元
            },
            "股票总数": len(df)
        }
        
        return json.dumps(result, ensure_ascii=False, indent=2)
        
    except Exception as e:
        # 返回模拟数据以便演示功能
        return json.dumps({
            "涨跌停统计": {
                "涨停家数": 45,
                "跌停家数": 12
            },
            "涨跌家数": {
                "上涨家数": 2856,
                "下跌家数": 1523,
                "平盘家数": 89
            },
            "市场表现": {
                "平均涨跌幅": 0.35,
                "总成交额": 7654.32
            },
            "股票总数": 4468,
            "note": "演示数据",
            "error": str(e)
        }, ensure_ascii=False, indent=2)


@tool
def get_sector_performance(runtime: ToolRuntime = None) -> str:
    """
    获取行业板块涨跌幅排行
    
    Returns:
        str: JSON 格式的行业板块表现数据
    """
    try:
        ctx = runtime.context if runtime else new_context(method="get_sector_performance")
        
        # 获取行业板块数据
        df = ak.stock_board_industry_name_em()
        
        # 提取需要的列
        columns = ['板块名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '上涨家数', '下跌家数']
        df = df[columns]
        
        # 按涨跌幅排序
        df = df.sort_values('涨跌幅', ascending=False)
        
        # 取前10名和后10名
        top_sectors = df.head(10)
        bottom_sectors = df.tail(10)
        
        result = {
            "表现最好的板块": [],
            "表现最差的板块": []
        }
        
        for _, row in top_sectors.iterrows():
            result["表现最好的板块"].append({
                "板块名称": row['板块名称'],
                "最新价": float(row['最新价']),
                "涨跌幅": float(row['涨跌幅']),
                "涨跌额": float(row['涨跌额']),
                "上涨家数": int(row['上涨家数']),
                "下跌家数": int(row['下跌家数'])
            })
        
        for _, row in bottom_sectors.iterrows():
            result["表现最差的板块"].append({
                "板块名称": row['板块名称'],
                "最新价": float(row['最新价']),
                "涨跌幅": float(row['涨跌幅']),
                "涨跌额": float(row['涨跌额']),
                "上涨家数": int(row['上涨家数']),
                "下跌家数": int(row['下跌家数'])
            })
        
        return json.dumps(result, ensure_ascii=False, indent=2)
        
    except Exception as e:
        # 返回模拟数据以便演示功能
        return json.dumps({
            "表现最好的板块": [
                {"板块名称": "AI应用", "最新价": 1256.78, "涨跌幅": 4.52, "涨跌额": 54.32, "上涨家数": 45, "下跌家数": 3},
                {"板块名称": "半导体", "最新价": 3456.89, "涨跌幅": 3.25, "涨跌额": 108.76, "上涨家数": 89, "下跌家数": 12},
            ],
            "表现最差的板块": [
                {"板块名称": "房地产", "最新价": 789.45, "涨跌幅": -2.15, "涨跌额": -17.34, "上涨家数": 8, "下跌家数": 67},
                {"板块名称": "银行", "最新价": 1023.56, "涨跌幅": -1.23, "涨跌额": -12.78, "上涨家数": 12, "下跌家数": 34},
            ],
            "note": "演示数据",
            "error": str(e)
        }, ensure_ascii=False, indent=2)


@tool
def get_stock_info(stock_code: str, runtime: ToolRuntime = None) -> str:
    """
    获取特定股票的详细信息
    
    Args:
        stock_code: 股票代码，如 "000001"（平安银行）
    
    Returns:
        str: JSON 格式的股票详细信息
    """
    try:
        ctx = runtime.context if runtime else new_context(method="get_stock_info")
        
        # 获取个股实时行情
        df = ak.stock_zh_a_spot_em()
        stock_data = df[df['代码'] == stock_code]
        
        if stock_data.empty:
            return f"未找到股票代码: {stock_code}"
        
        stock_data = stock_data.iloc[0]
        
        result = {
            "代码": stock_data['代码'],
            "名称": stock_data['名称'],
            "最新价": float(stock_data['最新价']),
            "涨跌幅": float(stock_data['涨跌幅']),
            "涨跌额": float(stock_data['涨跌额']),
            "今开": float(stock_data['今开']),
            "最高": float(stock_data['最高']),
            "最低": float(stock_data['最低']),
            "昨收": float(stock_data['昨收']),
            "成交量": float(stock_data['成交量']),
            "成交额": float(stock_data['成交额']),
            "换手率": float(stock_data['换手率']),
            "市盈率": float(stock_data['市盈率']),
            "市净率": float(stock_data['市净率']),
            "总市值": float(stock_data['总市值']),
            "流通市值": float(stock_data['流通市值'])
        }
        
        return json.dumps(result, ensure_ascii=False, indent=2)
        
    except Exception as e:
        # 返回模拟数据以便演示功能
        return json.dumps({
            "代码": stock_code,
            "名称": "示例股票",
            "最新价": 15.68,
            "涨跌幅": 5.23,
            "涨跌额": 0.78,
            "今开": 15.10,
            "最高": 15.85,
            "最低": 15.05,
            "昨收": 14.90,
            "成交量": 125680000,
            "成交额": 1987654321,
            "换手率": 3.45,
            "市盈率": 25.6,
            "市净率": 2.8,
            "总市值": 156800000000,
            "流通市值": 98760000000,
            "note": "演示数据",
            "error": str(e)
        }, ensure_ascii=False, indent=2)
