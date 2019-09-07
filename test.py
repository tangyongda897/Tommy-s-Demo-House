'''编写程序，输入一个字符串，将里面的字母大小写翻转，其他字符不变。ord()：根据字符获取编码值，chr()：根据编码值获取字符。'''
# str1=str(input('please input string:'))
# newstr=[]
# for item in str1:
#     if item.isupper():
#         newstr.append((item.lower()))
#     elif item.islower():
#         newstr.append(item.upper())
#     else:
#         newstr.append(item)
#
# for item in newstr:
#     print(item,end='')



'''
输入一个字符串，统计字符串中各个字符出现的次数。
输入年，月，日，计算这是这一年的第几天，例如输入2012年1月2日，计算结果是第2天。
打印出杨辉三角形（要求打印出 10 行）。
掷骰子10000次，统计得到各点数的次数。
编写程序，输入一个字符串，将里面的字母大小写翻转，其他字符不变。ord()：根据字符获取编码值，chr()：根据编码值获取字符。
随机将54张牌发到3个人手中，整理出每个人牌里炸弹（四张数字相同），三张（三张数字相同），对子（两张数字相同），单张的数量。
'''
#
# string1=input('please input: ')
# set1=set()
#
# for i in string1:
#     set1.add(i)
#
# for item in set1:
#     print(item,string1.count(item))


#输入字符串打印字符串每个字符出现次数
# string2=input('please input:')
# dict1=dict()
##如果字符不在字典里，则添加到字典，值为1
#如果字符在字典里，则取值+1
# for item in string2:
#     if item in dict1.keys():
#         dict1[item]=dict1[item]+1
#     else:
#         dict1[item]=1
#
# for k,v in dict1.items():
#     print(k,v)
#########################分解质因数
# my_num=int(input('please input :'))
# mid=[]
# i=2
# while i <= my_num:
#     if my_num%i==0:
#         mid.append(i)
#         my_num=my_num/i
#         if my_num==1:
#             break
#
#     else:
#         i+=1
# print(mid)

def fun(a,b):
    """

    :param a:
    :param b:
    :return:
    """
    c=a+b
    return c

print(fun('10','20'))
