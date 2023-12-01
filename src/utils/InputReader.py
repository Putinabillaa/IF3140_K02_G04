import re

from type.Operation import Operation, OperationType


# Valid Regex
# - R<number>(<item>)
# - W<number>(<item>)
# - C<number>

read_pattern = re.compile(r'R(\d+)\(([A-Z]+)\)')
write_pattern = re.compile(r'W(\d+)\(([A-Z]+)\)')
commit_pattern = re.compile(r'C(\d+)')


class InputReader():
    @staticmethod
    def read_schedule() -> list[Operation]:
        schedule: list[Operation] = []
        print('Format: R<number>(<item>) W<number>(<item>) C<number>')
        input_schedule = str(input('Enter schedule: '))
        input_schedule = input_schedule.split(' ')
        for idx, el in enumerate(input_schedule):
            if el == '':
                continue
            match: re.Match = read_pattern.match(el)
            if match:
                schedule.append(Operation(OperationType.READ, int(match.group(1)), match.group(2)))
                continue
            match: re.Match = write_pattern.match(el)
            if match:
                schedule.append(Operation(OperationType.WRITE, int(match.group(1)), match.group(2)))
                continue
            match: re.Match = commit_pattern.match(el)
            if match:
                schedule.append(Operation(OperationType.COMMIT, int(match.group(1))))
                continue
            raise Exception(f'Invalid input at operation {idx + 1}: {el}')

        return schedule
