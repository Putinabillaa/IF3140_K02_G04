from manager.MultiversionTimestamp import MultiversionTimestampManager
from manager.TwoPhaseLocking import TwoPhaseLockingManager
from manager.OptimisticConcurrencyControl import OptimisticConcurrencyControlManager
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
        cek = False
        while cek == False:
            file_path = input('Enter file path: ')
            try:
                schedule = FileReader.read_schedule(file_path)
                cek = True
            except Exception as e:
                print(e)
            

    elif choice == '2':
        cek = False
        while cek == False:
            try:
                schedule = InputReader.read_schedule()
                cek = True
            except Exception as e:
                print(e)
    
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
        manager = OptimisticConcurrencyControlManager()
    elif choice == '3':
        manager = MultiversionTimestampManager()

    manager.simulate(schedule)
