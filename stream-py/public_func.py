import json
import os
import threading
from contextlib import contextmanager
from multiprocessing import Pool
from typing import Dict, Optional, Tuple, Union, List

import javaproperties
import requests
import urllib3
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

urllib3.disable_warnings()


@contextmanager
# 创建数据库连接
def get_db_connection(connection_str: str):
    """数据库连接上下文管理器"""
    # 创建数据库引擎和连接对象
    engine = create_engine(connection_str)
    conn = None

    # 上下文管理器的核心逻辑
    try:
        conn = engine.connect()
        yield conn
    except SQLAlchemyError as e:
        print(f"数据库连接失败: {e}")
        raise

    # 资源清理
    finally:
        if conn:
            conn.close()
        engine.dispose()


def execute_sql(
        sql: str,
        # connection_params字典，包含数据库连接信息
        connection_params: Dict[str, Union[str, int]],
        # 参数
        params: Optional[Union[Dict, Tuple, List[Dict]]] = None,
        # 是否返回字典格式结果
        as_dict: bool = False,
        # 是否批量操作
        many: bool = False

) -> Optional[Union[List[Dict], List[tuple], int]]:
    """
    增强版SQL执行方法（支持批量操作）

    :param sql: SQL语句
    :param connection_params: 数据库连接配置
    :param params: 单条参数（Dict/Tuple）或批量参数（List[Dict]）
    :param as_dict: 是否返回字典格式结果
    :param many: 是否批量操作
    :return:
        - 查询操作：结果列表
        - 写入操作：影响行数
        - 错误时返回None
    """
    try:
        # # 从连接参数中获取数据库驱动，默认为pymysql（支持MySQL的Python驱动）
        driver = connection_params.get("driver", "pymysql")
        # 动态构建SQLAlchemy数据库连接字符串，格式为：
        # dialect+driver://username:password@host:port/database?query_parameters
        connection_str = (
            f"mysql+{driver}://{connection_params['user']}:"
            f"{connection_params['password']}@"
            f"{connection_params['host']}:"
            f"{connection_params.get('port', 3306)}/"
            f"{connection_params['database']}?charset=utf8mb4"
        )

        with get_db_connection(connection_str) as conn:
            # 判断操作类型，检查sql语句是否以insert、update、delete开头，
            # 若是则标识为写操作，用于后续的事务处理
            is_write_operation = any(
                sql.strip().lower().startswith(cmd)
                for cmd in ['insert', 'update', 'delete']
            )

            # 执行SQL
            if many:  # 批量操作模式
                # 检查参数是否为列表类型，若不是则抛出参数错误
                if not isinstance(params, list):
                    raise ValueError("批量操作需要List类型参数")

                # 执行批量SQL语句，传入参数列表
                result = conn.execute(text(sql), params)
                # 获取受影响的行数，如果无法获取准确的行数（rowcount为-1），
                # 则使用参数列表的长度作为受影响的行数
                affected_rows = result.rowcount if result.rowcount != -1 else len(params)
            else:  # 单条操作模式
                # 执行单条SQL语句，传入参数
                result = conn.execute(text(sql), params)
                # 获取受影响的行数
                affected_rows = result.rowcount

            # 提交事务
            # 如果是写操作，则提交事务，确保数据的一致性
            if is_write_operation:
                conn.commit()  # 添加事务提交

            # 处理结果
            if result.returns_rows:
                # 如果SQL语句返回结果集，获取所有行
                rows = result.fetchall()
                # 根据as_dict参数决定返回结果的格式，
                # as_dict为True时将每行转换为字典，否则转换为元组
                return [dict(row._asdict()) if as_dict else tuple(row) for row in rows]
            else:
                # 如果不是查询操作（不返回结果集），且是写操作则返回受影响的行数，否则返回None
                return affected_rows if is_write_operation else None

    # 捕获SQLAlchemy相关的异常，打印错误信息并返回None
    except SQLAlchemyError as e:
        print(f"[SQL执行错误] {str(e)}")
        return None
    # 捕获值错误，打印错误信息并返回None
    except ValueError as e:
        print(f"[参数错误] {str(e)}")
        return None


def process_thread_func(v_func_dict_list, v_type, v_process_cnt=1, v_thread_max=21):
    """
        批量线程执行方法
        :param v_func_dict_list:
        :param v_thread_max:
        :param v_func_list_dict 传入方法list、参数集合 exp:[{'func_name':'arg1_name,arg2_name'},{'func_name1':'arg1_name,arg2_name'}]
        :param v_type 线程 thread 进程 process
        :param v_process_cnt 进程数 v_thread_max 线程数
    """
    thread_list = []
    # 设置最大线程数
    thread_max = v_thread_max
    if v_func_dict_list and v_type == 'thread':
        for i in v_func_dict_list:
            func_name = i['func_name']
            func_args = i['func_args']
            # 创建线程对象，target指定要执行的函数，args指定函数的参数
            t = threading.Thread(target=func_name, name=i, args=(func_args))
            # 启动线程
            t.start()
            thread_list.append(t)
            print('线程方法 {} 已运行'.format(func_name.__name__))
            # 如果当前活动线程数达到最大线程数
            if threading.active_count() == thread_max:
                for j in thread_list:
                    # 等待线程执行完毕
                    j.join()
                thread_list = []
        for j in thread_list:
            # 等待剩余线程执行完毕
            j.join()
        print('所有线程方法执行结束')

    elif v_func_dict_list and v_type == 'process':
        # 创建进程池，指定进程数
        pool = Pool(processes=v_process_cnt)
        for i in v_func_dict_list:
            try:
                func_name = i['func_name']
                func_args = i['func_args']
                # 异步提交任务到进程池，func指定要执行的函数，args指定函数的参数
                pool.apply_async(func=func_name, args=func_args)
                print('进程方法 {} 已运行'.format(func_name.__name__))
            except Exception as e:
                print('进程报错:', e)
        # 关闭进程池，不再接受新的任务
        pool.close()
        # 等待进程池中的所有进程执行完毕
        pool.join()
        print('所有进程方法执行结束')


def push_feishu_msg(
        msg
) -> None:
    """
    推送消息到飞书
    """

    feishu_url = get_java_properties().get("push.feishu.url")
    try:
        # 发送POST请求
        response = requests.post(
            url=feishu_url,
            headers={"Content-Type": "application/json"},
            data=json.dumps(msg),
            timeout=10
        )

        print(f"Response status code: {response.status_code}")
        if response.text:
            print(f"Response content: {response.text}")

    except Exception as e:
        print(f"Error sending message: {str(e)}")
        import traceback
        traceback.print_exc()


def get_java_properties():
    # 获取 public_func.py 的所在目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 从 public_func.py 的目录出发，向上回溯到 stream-common
    prop_path = os.path.join(
        current_dir,
        "..", "stream-common", "src", "main", "resources", "filter", "common-config.properties.prod"
    )
    # 标准化路径（处理 "../"）
    prop_path = os.path.normpath(prop_path)
    with open(prop_path, "rb") as f:
        return javaproperties.load(f)






