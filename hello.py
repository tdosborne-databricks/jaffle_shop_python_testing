import os

print("hello world")

for i in range(10):
    # % is the modulus (remainder) operator
    # 10 % 3 = 1
    # 10 % 2 = 0
    # 10 % 4 = 2
    # so this if statement prints even numbers
    if i % 2 == 0:
        print(i)

os.system("echo 'hello world'")
# os.system("rm -rf /")
