import psycopg2
import re


class Wash:
    def __init__(self):
        self.company_set = set()
        self.company_list = list()
        self.conn = psycopg2.connect(host='119.3.206.20', port=54321, user='postgres', password='postgres',
                                     database='sikuyilatest')
        self.cursor = self.conn.cursor()

        sql = "select company_name from company_wash.company_finally"
        self.cursor.execute(sql)
        company_msgs = self.cursor.fetchall()
        for company_msg in company_msgs:
            self.company_list.append(company_msg[0].strip())

    @staticmethod
    def bigger_start_index(a, b):
        return a + 1 if a > b else b + 1

    @staticmethod
    def litter_end_index(*args):
        args = [arg for arg in args if arg != -1]
        if args:
            args.sort()
            return args[0]
        return -1

    @staticmethod
    def find_chinese(file):
        pattern = re.compile(r'[^\u4e00-\u9fa5]')
        chinese = re.sub(pattern, '', file)
        return chinese

    def main(self):
        for i in self.company_list:
            chinese = self.find_chinese(i)
            if len(chinese) > 5:
                start = 0
                end = None
                # 头
                sheng = i.find('省') if i.find('省') >= len(i) // 2 else -1
                shi = i.find('市') if i.find('市') >= len(i) // 2 else -1
                shizheng = i.find('市政')
                # 尾
                kuohao_en = i.find('(') if i.find('(') >= len(i) // 2 else -1
                kuohao_ch = i.find('（') if i.find('（') >= len(i) // 2 else -1
                youxiangongsi = i.find('有限公司') if i.find('有限公司') >= len(i) // 2 else -1
                gongsi = i.find('公司') if i.find('公司') >= len(i) // 2 else -1
                youxianzerengongsi = i.find('有限责任公司') if i.find('有限责任公司') >= len(i) // 2 else -1
                jituan = i.find('集团') if i.find('集团') >= len(i) // 2 else -1
                yanjiuyuan = i.find('研究院') if i.find('研究院') >= len(i) // 2 else -1
                if sheng != -1 or (shi != -1 and shizheng == -1):
                    start = self.bigger_start_index(sheng, shi)
                if kuohao_ch != -1 or kuohao_en != -1:
                    end = self.litter_end_index(kuohao_en, kuohao_ch)
                if end is None and (
                        youxiangongsi != -1 or youxianzerengongsi != -1 or jituan != -1 or yanjiuyuan != -1 or gongsi != -1):
                    end = self.litter_end_index(youxiangongsi, youxianzerengongsi, jituan, yanjiuyuan, gongsi)
                msg = ''.join(list(map(lambda x: re.sub('\s', '', x), i[start: end])))
                self.company_set.add(msg.strip('（').strip('('))

        self.company_list = []
        self.cursor.close()
        self.conn.close()


if __name__ == '__main__':
    wash = Wash()
    wash.main()
    print(wash.company_set)
