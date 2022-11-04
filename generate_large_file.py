import sys

def generate_big_random_letters(filename,size):
    """
    generate big random letters/alphabets to a file
    :param filename: the filename
    :param size: the size in bytes
    :return: void
    """
    import random
    import string

    chars = [random.choice(string.ascii_letters) for i in range(size)] #1

    c = len(chars)
    i = 100

    while (i < c):
        chars.insert(i, "\n")
        i += 100

    with open(filename, 'w') as f:
        f.write(''.join(chars))
    pass

if __name__ == '__main__':
    filename = sys.argv[1]
    size = sys.argv[2]
    generate_big_random_letters("data/" + filename, 1024 * 1024 * size)