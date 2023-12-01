from manager.MultiversionTimestamp import MultiversionTimestampManager
from manager.TwoPhaseLocking import TwoPhaseLockingManager
from utils.FileReader import FileReader
from utils.InputReader import InputReader

if __name__ == '__main__':
    print('Choose how to input schedule:')
    print('1. From file')
    print('2. From console')
    choice = input('>> ')
    while choice not in ['1', '2']:
        choice = input('>> ')

    if choice == '1':
        file_path = input('Enter file path: ')
        schedule = FileReader.read_schedule(file_path)

    elif choice == '2':
        schedule = InputReader.read_schedule()
    
    print('Choose algorithm:')
    print('1. Two Phase Locking')
    print('2. Optimistic Concurrency Control')
    print('3. Multi Version Concurrency Control')
    choice = input('>> ')
    while choice not in ['1', '2', '3']:
        choice = input('>> ')
    
    if choice == '1':
        manager = TwoPhaseLockingManager()
    elif choice == '2':
        # manager = OptimisticConcurrencyControlManager()
        print('Not implemented yet')
    elif choice == '3':
        manager = MultiversionTimestampManager()

    manager.simulate(schedule)