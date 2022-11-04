import sys


def main():
    file_path = sys.argv[1]
    try:
        chunk_length = int(sys.argv[2])
    except:
        chunk_length = 1000
    read_by_chunk(file_path, chunk_length)


def read_by_chunk(file, chunk_length):
    with open(file, encoding='utf-8') as f:
        while True:
            c = f.read(chunk_length)
            # print(('\n\n\n').join(c.split('\n')))
            if not c:
                print("End of file")
                break
            print(c)
            p = input('press enter to read more. x to quit.')
            if p == "x":
                break


if __name__ == "__main__":
    main()
