# Hash the name of the airport hash(name)
def GetHashValue(value):
    return hash(value)


# hash mod N (number of nodes) chaining completed
def GetHashKey(value, divider):
    hashValue = GetHashValue(value)
    return hashValue % divider


def get_data(start, stop, divider):  # Returns multiple lines of hashed data from airports file
    # Using readlines()
    file1 = open('airports.dat', encoding="utf8")
    Lines = file1.readlines()

    # Strips the newline character
    data_to_return = []
    for line in Lines:
        temp_line = line
        data_to_send = temp_line.replace("\n", "")
        # Replaces all the new line string and removes the comma
        data = data_to_send.split(',')
        if int(data[0]) < start:
            continue
        if int(data[0]) > stop:
            break
        # Hash the name of the airport
        tup = (GetHashKey(data[1], divider), GetHashValue(data[1]), data_to_send)
        data_to_return.append(tup)

    return data_to_return


def data_hash(data, divider):  # Returns hashed data for a single line of data
    temp_data = data.replace("\n", "")
    data = temp_data.split(',')
    tup = (GetHashKey(data[1], divider), GetHashValue(data[1]), temp_data)
    return tup
