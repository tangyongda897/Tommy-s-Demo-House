products=[['白菜',3.5],['萝卜',1],['牛肉',24],['猪肉',12],['开水',0]]
shopping_car=[]
total=0
flag=True

while flag:
    print('##########商品列表##############')
    for index,item in enumerate(products):
        print('{}.{} :{}'.format(index,item[0],item[1]))
    choice='4'
    print(choice)
    if choice.isdigit():
        choice = int(choice)
        shopping_car.append(products[choice][0])
        total=total+products[choice][1]
        print('{}已加入购物车,总计:{}元'.format(products[choice][0],total))
    elif choice=='q':
        print('####您要买的列表####')
        for index,item in enumerate(shopping_car):
            print('{}.{} :{}'.format(index,item[0],item))
            print('总计{}元'.format(total))
        break;
    else:
        print('商品不存在')

