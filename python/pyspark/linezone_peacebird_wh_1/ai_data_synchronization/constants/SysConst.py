#coding:utf-8

"""
@autho 王光华
@date 2017-05-25
系统常量定义类，常量定义类主要用于定义系统各类型常量。
通过建立一个const类，对其object.__setattr__()方法进行overwrite，在对属性值进行赋值的时候判断，
如果属性存在，则表示这是对常量的重赋值操作，从而抛出异常，如果属性不存在，则表示是新声明了一个常量，可以进行赋值操作。
"""


class _constants:

  class ConstError(TypeError):
      pass

  class ConstCaseError(ConstError):
      pass

  def __setattr__(self, name, value):
      if name in self.__dict__:
          raise self.ConstError("无法修改常量 %s" % name)
      if not name.isupper():
          raise self.ConstCaseError('常量名 "%s" 必须全部大写' % name)
      self.__dict__[name] = value
