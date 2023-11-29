import os
import re


from type.Transaction import Transaction, TransactionType


# Valid Regex
# - R<number>(<item>)
# - W<number>(<item>)
# - C<number>

read_pattern = re.compile(r'R(\d+)\(([A-Z]+)\)')
write_pattern = re.compile(r'W(\d+)\(([A-Z]+)\)')
commit_pattern = re.compile(r'C(\d+)')


class FileReader:
    @staticmethod
    def read_schedule(file_path: str):
        schedule: list[Transaction] = []

        with open(file_path, 'r') as file:
            for idx, line in enumerate(file.readlines()):
                line = line.strip()

                if line == '':
                    continue

                match: re.Match = read_pattern.match(line)
                if match:
                    schedule.append(Transaction(TransactionType.READ, match.group(1), match.group(2)))
                    continue

                match: re.Match = write_pattern.match(line)
                if match:
                    schedule.append(Transaction(TransactionType.WRITE, match.group(1), match.group(2)))
                    continue

                match: re.Match = commit_pattern.match(line)
                if match:
                    schedule.append(Transaction(TransactionType.COMMIT, match.group(1)))
                    continue

                raise Exception(f'Invalid line at line {idx + 1}: {line}')


        return schedule
