from os.path import dirname, abspath

print(dirname(dirname(abspath(__file__))))
print(dirname(abspath(__file__)))
