# IF3140_K02_G04
This repository implements simulation algorithms for various concurrency control protocols. 

The supported protocols include:

1. Two-Phase Locking (2PL)
2. Optimistic Concurrency Control (OCC)
3. Multiversion Timestamp Ordering Concurrency Control (MVCC)

Each algorithm is implemented in Python.

## Requirements
This program require python 3.10 or later versions to run.

## How to Use?
1. run main.py from the src directory.
    ```
    python main.py
    ```
2. Choose how do you want to input the input schedule: 1 for file, 2 for input from console.
    ```
    Choose how to input schedule:
    1. From file
    2. From console
    >> 
    ```
3. If you choose to input from file, enter the file path relative to scr directory, and follow this input format:
   
    ```
    R<number>(<item>)
    W<number>(<item>)
    C<number>
    ```
    , each operation separated by newline and no other characters. 

    If you choose to input from console, follow this input format: ```R<number>(<item>) W<number>(<item>) C<number>```, each operation separated by space and no other characters.

    Note: number should be an integer (representing transaction number) and item should be any string but not a number.
  5. Choose the algorithm you want to use.
     
      ```
      Choose algorithm:
      1. Two Phase Locking
      2. Optimistic Concurrency Control
      3. Multi Version Concurrency Control
      >>  
      ```
5. The final schedule will look like this:
   - For Two-Phase Locking (2PL):
     ```
      Final Schedule:
      SL1(X) --> Shared Lock
      R1(X)
      XL3(Y) --> Exclusive Lock
      W3(Y)
      C1
      XL2(X)
      W2(X)
      A3 --> Abort, rollback
      XL2(Y)
      W2(Y)
      C2
      XL3(Y)
      W3(Y)
      C3
     ```
    - For Optimistic Concurrency Control (OCC):
      ```
      Final Schedule:
      R1(A)
      T1 gets <A0, 0, 0>  and updates A0 <A0, 1, 0>
      --> A0 represents version 0 of resource A, 1 represents read timestamp of resource A,
      --> 0 represents write timestamp of resource A
      R2(A)
      T2 gets <A0, 1, 0>  and updates A0 <A0, 2, 0>
      R3(B)
      T3 gets <B0, 0, 0>  and updates B0 <B0, 3, 0>
      R1(B)
      T1 gets <B0, 3, 0> 
      W3(C)
      T3 gets <C0, 0, 0> and creates C1 <C1, 3, 3>
      W2(C)
      T2 gets <C0, 0, 0> and creates C2 <C2, 2, 2>
      R1(C)
      T1 gets <C0, 0, 0>  and updates C0 <C0, 1, 0>
      C1
      Transaction 1 committed
      R2(D)
      T2 gets <D0, 0, 0>  and updates D0 <D0, 2, 0>
      W3(B)
      T3 gets <B0, 3, 0> and creates B1 <B1, 3, 3>
      C2
      Transaction 2 committed
      C3
      Transaction 3 committed
       ```

   ## Contributors
      |NIM|Name|
      |---|---|
      |13521078|Christian Albert Hasiholan|
      |13521088|Puti Nabilla Aidira|
      |13521096|Noel Christoffel Simbolon|
      |1352111|Farhan Nabil Suryono|
        
