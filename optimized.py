import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.orm import declarative_base, sessionmaker
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='crawler.log'
)

# 数据库配置
DATABASE_URI = 'jdbc:mysql://localhost:3306'
Base = declarative_base()


class LicenseRecord(Base):
    __tablename__ = 'licenses'
    id = Column(Integer, primary_key=True)
    org_code = Column(String(20))
    org_name = Column(String(100))
    # 其他字段按需添加...


class DatabaseManager:
    def __init__(self):
        self.engine = create_engine(DATABASE_URI)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def save_record(self, record_data):
        session = self.Session()
        try:
            record = LicenseRecord(**record_data)
            session.add(record)
            session.commit()
        except Exception as e:
            logging.error(f"Database error: {str(e)}")
            session.rollback()
        finally:
            session.close()


# 爬虫核心类
class LicenseCrawler:
    def __init__(self, target_ids):
        self.task_queue = Queue()
        self.db = DatabaseManager()
        self._init_task_queue(target_ids)
        self.driver_pool = self._init_driver_pool()

    def _init_driver_pool(self):
        """创建浏览器实例池"""
        return [self._create_driver() for _ in range(4)]

    def _create_driver(self):
        """创建单个浏览器实例"""
        options = webdriver.EdgeOptions()
        options.add_argument("--headless")
        driver = webdriver.Edge(options=options)
        driver.set_page_load_timeout(30)
        return driver

    def _init_task_queue(self, ids):
        """初始化乱序任务队列"""
        shuffled_ids = random.sample(ids, len(ids))
        for _id in shuffled_ids:
            self.task_queue.put(_id)

    def _random_delay(self):
        """生成1-5秒随机延迟"""
        delay = random.uniform(1, 5)
        time.sleep(delay)

    def _fetch_data(self, driver, license_id):
        """执行单个页面抓取"""
        try:
            url = f"https://example.com/license/{license_id}"
            driver.get(url)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, 'tbody'))
            )
            # 数据解析逻辑...
            return {"id": license_id, "data": "parsed_data"}
        except Exception as e:
            logging.warning(f"Fetch failed for ID {license_id}: {str(e)}")
            return None

    def worker(self):
        """多线程工作单元"""
        while not self.task_queue.empty():
            license_id = self.task_queue.get()
            driver = random.choice(self.driver_pool)

            try:
                self._random_delay()
                result = self._fetch_data(driver, license_id)
                if result:
                    self.db.save_record(result)
                    logging.info(f"Success: ID {license_id}")
            finally:
                self.task_queue.task_done()

    def run(self, max_workers=8):
        """启动爬虫引擎"""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for _ in range(max_workers):
                executor.submit(self.worker)


if __name__ == "__main__":
    # 示例使用：从文件读取目标ID
    with open("target_ids.txt") as f:
        target_ids = [int(line.strip()) for line in f]

    crawler = LicenseCrawler(target_ids)
    crawler.run(max_workers=6)