# import os 
# 参考链接： https://www.cnblogs.com/xinchrome/p/5011304.html

# 获取文件所在的文件夹名称
# print(os.getcwd())
# 删除文件os.remove(filename)
# 文件重命名 os.rename(old_file_name, new_file_name)
# 切换目录 os.chdir("newdir")切换到newdir文件夹
# 新建目录 os.mkdir("newdir")
# 删除空目录 os.rmdir("newdir")
# 文件定位 tell()方法告诉你文件内的当前位置, 换句话说，下一次的读写会发生在文件开头这么多字节之后。
# seek（offset [,from]）方法改变当前文件的位置。Offset变量表示要移动的字节数。From变量指定开始移动字节的参考位置。
# 如果from被设为0，这意味着将文件的开头作为移动字节的参考位置。如果设为1，则使用当前的位置作为参考位置。如果它被设为2，那么该文件的末尾将作为参考位置。
# 一次性读文件所有内容到内存中f.read()
# 一次性读文件n个字节到内存中f.read(n),可以和seek方法配合，实现从执行位置开始读取指定字节的数据。
# 写入文件f.write(msg_str) 注意Python字符串可以是二进制数据，而不是仅仅是文字。write()方法不会在字符串的结尾添加换行符('\n')：
# 关闭文件 File 对象的 close（）方法刷新缓冲区里任何还没写入的信息，并关闭该文件，这之后便不能再进行写入。
# 当一个文件对象的引用被重新指定给另一个文件时，Python 会关闭之前的文件。用 close（）方法关闭文件是一个很好的习惯。
# 文件对象的属性
# file.closed	返回true如果文件已被关闭，否则返回false。
# file.mode	返回被打开文件的访问模式。
# file.name	返回文件的名称。
# file.softspace	如果用print输出后，必须跟一个空格符，则返回false。否则返回true。
# 文件常见模式
# w 先清空文件内容再往里面写数据；若文件不存在则创建
# a 往文件末尾追加数据，如果该文件不存在，创建新文件进行写入。
# b 二进制模式
# + 打开一个文件进行更新(可读可写)。
# r 只读方式打开文件，文件指针在文件的开头；如果不指定模式，默认是r
# rb 用二进制个事打开一个只读文件，文件指针在文件开头，一般用于非文本文件如图片等，默认模式。
# r+	打开一个文件用于读写。文件指针将会放在文件的开头。
# rb+	以二进制格式打开一个文件用于读写。文件指针将会放在文件的开头。一般用于非文本文件如图片等。
# wb	以二进制格式打开一个文件只用于写入。如果该文件已存在则打开文件，并从开头开始编辑，即原有内容会被删除。如果该文件不存在，创建新文件。一般用于非文本文件如图片等。
# w+	打开一个文件用于读写。如果该文件已存在则打开文件，并从开头开始编辑，即原有内容会被删除。如果该文件不存在，创建新文件。
# wb+	以二进制格式打开一个文件用于读写。如果该文件已存在则打开文件，并从开头开始编辑，即原有内容会被删除。如果该文件不存在，创建新文件。一般用于非文本文件如图片等。
# ab	以二进制格式打开一个文件用于追加。如果该文件已存在，文件指针将会放在文件的结尾。也就是说，新的内容将会被写入到已有内容之后。如果该文件不存在，创建新文件进行写入。
# a+	打开一个文件用于读写。如果该文件已存在，文件指针将会放在文件的结尾。文件打开时会是追加模式。如果该文件不存在，创建新文件用于读写。
# ab+	以二进制格式打开一个文件用于追加。如果该文件已存在，文件指针将会放在文件的结尾。如果该文件不存在，创建新文件用于读写。

# 得到当前工作目录，即当前Python脚本工作的目录路径: os.getcwd()

# 返回指定目录下的所有文件和目录名:os.listdir()

# 函数用来删除一个文件:os.remove()

# 删除多个目录：os.removedirs（r“c：\python”）

# 检验给出的路径是否是一个文件：os.path.isfile()

# 检验给出的路径是否是一个目录：os.path.isdir()

# 判断是否是绝对路径：os.path.isabs()

# 检查是否快捷方式os.path.islink ( filename )

# 检验给出的路径是否真地存:os.path.exists()

# 返回一个路径的目录名和文件名:os.path.split() eg os.path.split('/home/swaroop/byte/code/poem.txt') 结果：('/home/swaroop/byte/code', 'poem.txt')

# 分离扩展名：os.path.splitext()

# 获取路径名：os.path.dirname()

# 获取文件名：os.path.basename()

# 运行shell命令: os.system()

# 读取和设置环境变量:os.getenv() 与os.putenv()

# 给出当前平台使用的行终止符:os.linesep Windows使用'\r\n'，Linux使用'\n'而Mac使用'\r'

# 指示你正在使用的平台：os.name 对于Windows，它是'nt'，而对于Linux/Unix用户，它是'posix'

# 重命名：os.rename（old， new）

# 创建多级目录：os.makedirs（r“c：\python\test”）

# 创建单个目录：os.mkdir（“test”）

# 获取文件属性：os.stat（file）

# 修改文件权限与时间戳：os.chmod（file）

# 终止当前进程：os.exit（）

# 获取文件大小：os.path.getsize（filename）


# 文件操作：
# os.mknod("test.txt") 创建空文件
# fp = open("test.txt",w) 直接打开一个文件，如果文件不存在则创建文件

# 关于open 模式：

# w 以写方式打开，
# a 以追加模式打开 (从 EOF 开始, 必要时创建新文件)
# r+ 以读写模式打开
# w+ 以读写模式打开 (参见 w )
# a+ 以读写模式打开 (参见 a )
# rb 以二进制读模式打开
# wb 以二进制写模式打开 (参见 w )
# ab 以二进制追加模式打开 (参见 a )
# rb+ 以二进制读写模式打开 (参见 r+ )
# wb+ 以二进制读写模式打开 (参见 w+ )
# ab+ 以二进制读写模式打开 (参见 a+ )

 

# fp.read([size]) #size为读取的长度，以byte为单位

# fp.readline([size]) #读一行，如果定义了size，有可能返回的只是一行的一部分

# fp.readlines([size]) #把文件每一行作为一个list的一个成员，并返回这个list。其实它的内部是通过循环调用readline()来实现的。如果提供size参数，size是表示读取内容的总长，也就是说可能只读到文件的一部分。

# fp.write(str) #把str写到文件中，write()并不会在str后加上一个换行符

# fp.writelines(seq) #把seq的内容全部写到文件中(多行一次性写入)。这个函数也只是忠实地写入，不会在每行后面加上任何东西。

# fp.close() #关闭文件。python会在一个文件不用后自动关闭文件，不过这一功能没有保证，最好还是养成自己关闭的习惯。 如果一个文件在关闭后还对其进行操作会产生ValueError

# fp.flush() #把缓冲区的内容写入硬盘

# fp.fileno() #返回一个长整型的”文件标签“

# fp.isatty() #文件是否是一个终端设备文件（unix系统中的）

# fp.tell() #返回文件操作标记的当前位置，以文件的开头为原点

# fp.next() #返回下一行，并将文件操作标记位移到下一行。把一个file用于for … in file这样的语句时，就是调用next()函数来实现遍历的。

# fp.seek(offset[,whence]) #将文件打操作标记移到offset的位置。这个offset一般是相对于文件的开头来计算的，一般为正数。但如果提供了whence参数就不一定了，whence可以为0表示从头开始计算，1表示以当前位置为原点计算。2表示以文件末尾为原点进行计算。需要注意，如果文件以a或a+的模式打开，每次进行写操作时，文件操作标记会自动返回到文件末尾。

# fp.truncate([size]) #把文件裁成规定的大小，默认的是裁到当前文件操作标记的位置。如果size比文件的大小还要大，依据系统的不同可能是不改变文件，也可能是用0把文件补到相应的大小，也可能是以一些随机的内容加上去。

 

# 目录操作：
# os.mkdir("file") 创建目录
# 复制文件：
# shutil.copyfile("oldfile","newfile") oldfile和newfile都只能是文件
# shutil.copy("oldfile","newfile") oldfile只能是文件夹，newfile可以是文件，也可以是目标目录
# 复制文件夹：
# shutil.copytree("olddir","newdir") olddir和newdir都只能是目录，且newdir必须不存在
# 重命名文件（目录）
# os.rename("oldname","newname") 文件或目录都是使用这条命令
# 移动文件（目录）
# shutil.move("oldpos","newpos") 
# 删除文件
# os.remove("file")
# 删除目录
# os.rmdir("dir")只能删除空目录
# shutil.rmtree("dir") 空目录、有内容的目录都可以删
# 转换目录
# os.chdir("path") 换路径

 

# ps: 文件操作时，常常配合正则表达式：

# img_dir = img_dir.replace('\\','/')

# ===============================================================================================

#  存储对象

 

 

# 使用前一节中介绍的模块，可以实现在文件中对字符串的读写。 
# 然而，有的时候，你可能需要传递其它类型的数据，如list、tuple、dictionary和其它对象。在Python中，你可以使用Pickling来完成。你可以使用Python标准库中的“pickle”模块完成数据编组。 
# 下面，我们来编组一个包含字符串和数字的list：

# 1. import pickle 
# 2. 
# 3. fileHandle = open ( 'pickleFile.txt', 'w' ) 
# 4. testList = [ 'This', 2, 'is', 1, 'a', 0, 'test.' ] 
# 5. pickle.dump ( testList, fileHandle ) 
# 6. fileHandle.close()

# import pickle

# fileHandle = open ( 'pickleFile.txt', 'w' ) 
# testList = [ 'This', 2, 'is', 1, 'a', 0, 'test.' ] 
# pickle.dump ( testList, fileHandle ) 
# fileHandle.close()

# 拆分编组同样不难：

# 1. import pickle 
# 2. 
# 3. fileHandle = open ( 'pickleFile.txt' ) 
# 4. testList = pickle.load ( fileHandle ) 
# 5. fileHandle.close()

# import pickle

# fileHandle = open ( 'pickleFile.txt' ) 
# testList = pickle.load ( fileHandle ) 
# fileHandle.close()

# 现在试试存储更加复杂的数据：

# 1. import pickle 
# 2. 
# 3. fileHandle = open ( 'pickleFile.txt', 'w' ) 
# 4. testList = [ 123, { 'Calories' : 190 }, 'Mr. Anderson', [ 1, 2, 7 ] ] 
# 5. pickle.dump ( testList, fileHandle ) 
# 6. fileHandle.close()

# import pickle

# fileHandle = open ( 'pickleFile.txt', 'w' ) 
# testList = [ 123, { 'Calories' : 190 }, 'Mr. Anderson', [ 1, 2, 7 ] ] 
# pickle.dump ( testList, fileHandle ) 
# fileHandle.close()

# 1. import pickle 
# 2. 
# 3. fileHandle = open ( 'pickleFile.txt' ) 
# 4. testList = pickle.load ( fileHandle ) 
# 5. fileHandle.close()

# import pickle

# fileHandle = open ( 'pickleFile.txt' ) 
# testList = pickle.load ( fileHandle ) 
# fileHandle.close()


# 如上所述，使用Python的“pickle”模块编组确实很简单。众多对象可以通过它来存储到文件中。如果可以的话，“cPickle”同样胜任这个工作。它和“pickle”模块一样，但是速度更快：

# 1. import cPickle 
# 2. 
# 3. fileHandle = open ( 'pickleFile.txt', 'w' ) 
# 4. cPickle.dump ( 1776, fileHandle ) 
# 5. fileHandle.close()

# import cPickle

# fileHandle = open ( 'pickleFile.txt', 'w' ) 
# cPickle.dump ( 1776, fileHandle ) 
# fileHandle.close()

 

 

# 字符串匹配

# 对于简单的数据，使用流文本文件而不是数据库更简单明了，也就少不了文件操作和字符串匹配的需求。

# re模块的search和match方法是匹配到就返回，而不是去匹配所有，而findall()则匹配所有返回数组。
# 　>>> m=re.findall("^a\w+","abcdfa\na1b2c3",re.MULTILINE)
# 　>>> m
# 　['abcdfa', 'a1b2c3']