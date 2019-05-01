"""
用smtp发邮件和pop收邮件
author：xhw
last_modified:2019-04-23
问题:附件的后缀无法判断，有时出现无后缀的情况
"""
from email.mime.text import MIMEText
from email.header import Header
import smtplib
from email.utils import parseaddr, formataddr
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.parser import Parser
from email.header import decode_header
import poplib
import sys
import pprint
poplib._MAXLINE=20480

class email_demo(object):
    sender_smtp = {}
    receiver_pop = {}
    def __init__(self, sender, target, smtp_server, pop_server, receiver):
        if sender["email"] is None or sender["pass"]  is None:
            print("用户名密码不允许为空！")
            sys.exit()
        smtp_demo.sender_smtp['email_name'] = sender["email"]
        smtp_demo.sender_smtp['pass_word'] = sender["pass"]

        smtp_demo.receiver_pop['email_name'] = receiver["email"]
        smtp_demo.receiver_pop['pass_word'] = receiver["pass"]
        self.target = target
        self.smtp_server = smtp_server # "smtp.exmail.qq.com"
        self.pop_server = pop_server

    @classmethod
    def _format_addr(self, s):
        # 这个函数的作用是把一个标头的用户名编码成utf-8格式的，如果不编码原标头中文用户名，用户名将无法被邮件解码
        name, addr = parseaddr(s)
        return formataddr((Header(name, "utf-8").encode(), addr))
    # Header().encode(splitchars=';, \t', maxlinelen=None, linesep='\n')
    # 功能：编码一个邮件标头，使之变成一个RFC兼容的格式
    @staticmethod
    def send_email(self,):
        # 接下来定义发送文件
        server = smtplib.SMTP_SSL(self.smtp_server, 465)
        # qq邮箱使用SSL连接，端口为465
        server.set_debuglevel(1)
        # SMTP.set_debuglevel(level)：设置是否为调试模式。默认为False，即非调试模式，
        # 非调试模式：表示不输出任何调试信息。
        server.login(smtp_demo.sender_smtp['email_name'], smtp_demo.sender_smtp['pass_word'])
        # 登陆到smtp服务器。
        # 现在几乎所有的smtp服务器，都必须在验证用户信息合法之后才允许发送邮件。
        msg = MIMEMultipart()
        msg["From"] = self._format_addr("发件人 <%s>" % smtp_demo.sender_smtp['email_name'])
        msg["To"] = self._format_addr("收件人 <%s>" % self.target)
        msg["Subject"] = Header("Python SMTP发邮件", "utf-8").encode()
        # 定义邮件正文
        # 接下来定义邮件本身的内容
        msg.attach(MIMEText("测试邮件", "plain", "utf-8"))
        # class email.mime.text.MIMEText(_text[, _subtype[, _charset]])：MIME文本对象，
        # 其中, _text是邮件内容，
        # 其中, _subtype邮件类型(MIME类型)，可以是text/plain（普通文本邮件），html/plain(html邮件),
        # 其中, _charset编码（charset：字符集），可以是gb2312等等。
        # message.attch(payload) 将给定的附件或信息，添加到已有的有效附件或信息中，在调用之前必须是None或者List，调用后。payload将变成信息对象的列表
        # 如果你想将payload设置成一个标量对象，要使用set_payload()
        with open(r'D:\doc\xhw\xhw_tmp\code\python\scapy_pic\NiTu\11_石家庄电视塔日落.jpg', 'rb') as f:
            mime = MIMEBase("image", "jpg", filename="test.jpg")
            mime.add_header("Content-Disposition", "attchment", filename="test.jpg")
            # add_header(_name, _value, **_params) 扩展标头设置
                # _name：要添加的标头字段
                # _value：标头的内容
            # Content-Disposition就是当用户想把请求所得的内容存为一个文件的时候提供一个默认的文件名。
                # 希望某类或者某已知MIME 类型的文件（比如：*.gif;*.txt;*.htm）能够在访问时弹出“文件下载”对话框。
            mime.add_header("Content-ID", "<0>")
            mime.add_header("X-Attachment-Id", "0")
            mime.set_payload(f.read())
            # set_payload(payload, charset=None)
                # 将附件添加到payload中
            encoders.encode_base64(mime)
            # encoders.encode_base64(mime) 将payload内容编码为base64格式
            msg.attach(mime)
        # 构造附件1，传送当前目录下的 test.txt 文件
        att1 = MIMEText(open('test.txt', 'rb').read(), 'base64', 'utf-8')
        att1["Content-Type"] = 'application/octet-stream'
        # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
        att1["Content-Disposition"] = 'attachment; filename="test.txt"'
        msg.attach(att1)

        xlsx_path = r"D:\doc\download\tencent\WeChat Files\xhw18846419675\Files\edw_ai.xlsx"
        att2 = MIMEText(open(xlsx_path, 'rb').read(), 'base64', 'utf-8')
        att2["Content-Type"] = 'application/octet-stream'
        # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
        att2["Content-Disposition"] = 'attachment; filename="edw_ai.xlsx"'
        msg.attach(att2)

        server.sendmail(smtp_demo.sender_smtp['email_name'], self.target, msg.as_string())
        # SMTP.sendmail(from_addr, to_addrs, msg[, mail_options, rcpt_options]) ：发送邮件。
        # 这里要注意一下第三个参数，msg是字符串，表示邮件。
        # 我们知道邮件一般由标题，发信人，收件人，邮件内容，附件等构成，
        # 发送邮件的时候，要注意msg的格式。这个格式就是smtp协议中定义的格式。
        # 第二个参数，接受邮箱为一个列表，表示可以设置多个接受邮箱
        # as_string()是MIMEMessage对象的一个方法，表示把MIMETest对象变成str
        server.quit()
        # 断开与smtp服务器的连接，相当于发送"quit"指令。

    @staticmethod
    def receive_email(self,):
        # 连接到POP3服务器
        server = poplib.POP3_SSL(self.pop_server)
        # 注意qq邮箱使用SSL连接
        # 打开调试信息
        server.set_debuglevel(1)
        # 打印POP3服务器的欢迎文字
        print(server.getwelcome().decode("utf-8"))
        # 身份认证
        server.user(smtp_demo.sender_smtp['email_name'])
        server.pass_(smtp_demo.sender_smtp['pass_word'])

        # stat()返回邮件数量和占用空间
        print("信息数量：%s 占用空间 %s" % server.stat())
        # list()返回(response, ['mesg_num octets', ...], octets)，第二项是编号
        resp, mails, octets = server.list()
        # print("list!!!!",server.list())
        # 返回的列表类似[b'1 82923', b'2 2184', ...]
        # print(mails)
        # 获取最新一封邮件，注意索引号从1开始
        # POP3.retr(which) 检索序号which的真个消息，然后设置他的出现标志 返回(response, ['line', ...], octets)这个三元组
        index = len(mails)
        resp, lines, ocetes = server.retr(index-1)

        # lines 存储了邮件的原始文本的每一行
        # 可以获得整个邮件的原始文本
        msg_content = b"\r\n".join(lines).decode('utf-8', 'ignore')
        # 稍后解析出邮件
        msg = Parser().parsestr(msg_content)
        # email.Parser.parsestr(text, headersonly=False)
        # 与parser()方法类似，不同的是他接受一个字符串对象而不是一个类似文件的对象
        # 可选的headersonly表示是否在解析玩标题后停止解析，默认为否
        # 返回根消息对象
        # 关闭连接
        # print(self.decode_str(msg.get('Subject')))
        for part in msg.walk():
            filename = part.get_filename()
            # print(filename)
            if filename:
                filename = self.decode_str(filename)
                print(filename)
                #print(filename,'       ',self.yestoday, self.yestoday in filename)
                data = part.get_payload(decode=True)
                # abs_filename = filename
                attach = open(filename+'.xlsx', 'wb')
                attach.write(data)
                attach.close()
        print("ko")
        server.quit()
        #### 解析邮件
    @classmethod
    def decode_str(self, msg_str):
        value, charset = decode_header(msg_str)[0]
        if charset:
            if charset == 'gb2312':
                charset = 'gb18030'
            value = value.decode(charset)
        return value
    # @staticmethod
    # def decode_str(s):
    #     # 邮件的Subject或者Email中包含的名字都是经过编码后的str，要正常显示，就必须decode
    #     value, charset = decode_header(s)[0]
    #     # decode_header()返回一个list，因为像Cc、Bcc这样的字段可能包含多个邮件地址，所以解析出来的会有多个元素。上面的代码我们偷了个懒，只取了第一个元素。
    #     if charset:
    #         value = value.decode(charset)
    #     return value

    @staticmethod
    def guess_charset(msg):
        # 文本邮件的内容也是str，还需要检测编码，否则，非UTF-8编码的邮件都无法正常显示
        charset = msg.get_charset()
        if charset is None:
            content_type = msg.get('Content-Type', '').lower()
            pos = content_type.find('charset=')
            if pos >= 0:
                charset = content_type[pos + 8:].strip()
        return charset

    @classmethod
    def print_info(msg, indent=0):
        # 解析邮件与构造邮件的步骤正好相反
        if indent ==0:
            for header in ["From", "To", "Subject"]:
                value = msg.get(header, "")
                if value:
                    if header == "Subject":
                        vaule = decode_str(value)
                    else:
                        hdr, addr =parseaddr(value)
                        name = decode_str(hdr)
                        value = u"%s <%s>" % (name, addr)
                print("%s%s:%s" % ("  " * indent, header, value))
            if (msg.is_multipart()):
                parts = msg.get_payload()
                for n, part in enumerate(parts):
                    print('%spart %s' % ('  ' * indent, n))
                    print('%s--------------------' % ('  ' * indent))
                    print_info(part, indent + 1)
            else:
                content_type = msg.get_content_type()
                if content_type=='text/plain' or content_type=='text/html':
                    content = msg.get_payload(decode=True)
                    charset = guess_charset(msg)
                    if charset:
                        content = content.decode(charset)
                    print('%sText: %s' % ('  ' * indent, content + '...'))
                else:
                    print('%sAttachment: %s' % ('  ' * indent, content_type))

if __name__ == "__main__":
    sender = {}
    # user_name = 'xinmeixin@linezonedata.com'
    sender["email"] = 'xinmeixin@linezonedata.com'
    sender["pass"] = 'ABCabc123456'
    # pass_ = 'ABCabc123456'
    receiver = {}
    receiver["email"] = 'xhwhit@gmail.com'
    receiver["pass"] = "xie04hong10"
    smtp_server = "smtp.exmail.qq.com"
    pop_server = "pop.exmail.qq.com"
    obj = smtp_demo(sender,['xhwhit@gmail.com'], smtp_server, pop_server, receiver)
    smtp_demo.receive_email(obj)

# email模块负责构造邮件
# 类email.mime.text.MIMEText(_text)，是使用字符串_text来生成MIME对象的主体文本
# MIME是(Multipurpose Internet Mail Extensions) 多用途互联网邮件扩展类型
# MIME是设置将某种扩展名文件用一种应用程序来打开的方式类型
# MIME设置的目的是为了在发送电子邮件时附加多媒体数据，让邮件根据其类型进行处理。

# smtplib模块负责发送邮件
# 类smtplib.SMTP([host[, port[, local_hostname[, timeout]]]]) ：SMTP对象
    # 其中，host：smtp服务器主机名
    # 其中，port：smtp服务器的端口，默认是25
# 如果在创建SMTP对象时定义了这两个参数，在初始化时会自动调用connect方式连接服务器
# smtplib模块还提供了SMTP_SSL类和LMTP类，对它们的操作与SMTP基本一致。
# SSL是一种安全传输，LMTP是与SMTP不同的另一种传输协议

# 如果你想让你的邮件标题使用非ASCII字符集，就要使用email.header编码非ASCII字符集
# email.header.Header(s=None, charset=None, maxlinelen=None, header_name=None, continuation_ws=' ', errors='strict')
    # 创建一个能容纳不同字符集的字符串的MIME对象的标头
    # 其中，s：初始标头，即要编码之前的标头
    # 其中，chatset：字符集，默认为ASCII
    # 其中，maxlinelen：标头名的行的最大长度，默认为76
    # 其中，header_name：标头名，默认无
    # 其中，continuation_ws：默认为单个空格字符
    # 其中，errors：直接传递到Header的append()方法里
# email.utils:杂项工具
# email.utils.parseaddr(address):解析地址的功能，
    # 其中，address是一个包含用户名和email地址的值（realname<address>），返回一个二元组(realname, email address)
# email.utils.formataddr(pair, charset='utf-8')
    # 其中，pair是二元组(realname, email address)
    # 其中，charset是字符串，默认为utf-8
# 实际上，parseaddr(), formataddr(),两者互逆
# email.mime.base.MIMEBase(_maintype, _subtype, **_params)
    # 这是MIME的一个基类。一般不需要在使用时创建实例。
    # 其中_maintype是内容类型，如text或者image。
    # _subtype是内容的minor type 类型，如plain或者gif。
    # **_params是一个字典，直接传递给Message.add_header()。
# email.mime.multipart.MIMEMultipart(_subtype='mixed', boundary=None, _subparts=None, **_params)
    # MIMEMultipart是MIMEBase的一个子类，多个MIME对象的集合
    # _subtype默认值为mixed。boundary是MIMEMultipart的边界，默认边界是可数的。
# email.encoders 功能是编码器
