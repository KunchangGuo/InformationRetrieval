from functools import reduce

def gamma_encoding(postings): return "".join([get_length(get_offset(gap))+get_offset(gap) for gap in get_gaps_list(postings)])
def gamma_decoding(gamma):
	num,length,offset,aux,res = 0,"","",0,[]
	while gamma!="":
		aux    = gamma.find("0")
		length = gamma[:aux]
		if length=="": res.append(1); gamma = gamma[1:]
		else:
			offset = "1"+gamma[aux+1:aux+1+unary_decodification(length)]
			res.append(int(offset,2))
			gamma  = gamma[aux+1+unary_decodification(length):]
	return res
	
def get_offset(gap): return bin(gap)[3:]    # 去掉0b和首位

def get_length(offset): return unary_codification(len(offset))+"0"

def unary_codification(gap):  return "".join(["1" for _ in range(gap)])

def unary_decodification(gap): return reduce(lambda x,y : int(x)+int(y),list(gap))

def get_gaps_list(posting_lists): return [posting_lists[0]]+[posting_lists[i]-posting_lists[i-1] for i in range(1,len(posting_lists))]

if __name__ == '__main__':
	print(gamma_encoding([10,15,22,23,34,44,50,58]))
	print(gamma_decoding(gamma_encoding([10,15,22,23,34,44,50,58])))
    
from functools import reduce 

# input: decimal number list 
# output: binary string gamma_encoded
def gamma_encode(num_list):
    binary_string = ''
    for num in num_list:
        binary_num_left = bin(num)[3:]  # delete 0b and first 1
        binary_string += unary_encode(len(binary_num_left)) + binary_num_left
    return binary_string


# input: binary_string
# output: list of number
def gamma_decode(binary_string):
    num_list = []
    while binary_string != "" :
        first_zero_pos = binary_string.find('0')
        binary_num_left_length = unary_decode(binary_string[:first_zero_pos+1])
        if binary_num_left_length == 0:
            num_list.append(0)
        else:
            binary_num = '1' + binary_string[first_zero_pos + 1 : first_zero_pos + 1 + binary_num_left_length]
            num = int(binary_num, 2)
            num_list.append(num)
        binary_string = binary_string[first_zero_pos + 1 + binary_num_left_length :]
    return num_list

        
# input: a decimal number
# output: number unary_encoded
def unary_encode(num):
    return ''.join('1'*num + '0')


# input: num_string unary_encoded
# output: corresponding number
def unary_decode(unary_num_string):
    return len(unary_num_string) - 1

print(gamma_encode([10,100]))

print(gamma_decode('11100101111110100100'))

print(len('11100101111110100100'))

print(unary_decode('110'))

print('1110'.find('1'))

print(array.array('B', '100').tobytes())

print(min(1,2))

print(encode([10, 100, 130]))

print(decode(encode([10, 120, 130])))

print('0'*5)

import array
def encode(postings_list):
    gap_list = postings_list.copy()
    for i in range(1, len(gap_list))[::-1]:
        gap_list[i] -= gap_list[i-1]
    gap_list_binary_string = gamma_encode(gap_list)
    if len(gap_list_binary_string) % 8 != 0:
        gap_list_binary_string += '0' * (8 - len(gap_list_binary_string) % 8)    # make it times of 8
    bytes = []
    while gap_list_binary_string != '':
        bytes.append(int(gap_list_binary_string[ : 8], 2))
        gap_list_binary_string = gap_list_binary_string[8 : ]
    return array.array('B', bytes).tobytes()


def decode(encoded_postings_list):
    gamma_encoded_list = array.array('B')
    gamma_encoded_list.frombytes(encoded_postings_list)
    gamma_encoded_list = gamma_encoded_list.tolist()
    binary_string = ''
    for num in gamma_encoded_list:
        binary_string += bin(num)[2:]
    postings_list = gamma_decode(binary_string)
    end_pos = 1
    for i in range(1, len(postings_list)):
        if postings_list[i] == 0:    # gap is 0, duplicated
            end_pos = i
            break
        postings_list[i] += postings_list[i-1]
    return postings_list[:end_pos]
