import asyncio
import sys
import re
import time
from datetime import datetime, timedelta, timezone

# 北京时间（UTC+8），只定义一次，避免每次创建
北京时间 = timezone(timedelta(hours=8))

from astrbot.api.all import Star, logger, Plain, At, AstrMessageEvent, filter, ProviderRequest
from astrbot.core.agent.message import TextPart
from collections import defaultdict

from astrbot.core.message.components import Poke, Reply, Image
from astrbot.core.star import Context
会话已唤醒键 = "消息补获_会话已唤醒"
会话主动停止键 = "消息补获_主动停止"
已使用工具等待键 = "消息补获_已使用工具等待"

class 消息补获(Star):

    def __init__(self, context: Context, config: dict):
        super().__init__(context)
        self.context = context
        self.自动触发正则列表:list[str] = config['自动触发正则列表']  # 别怀疑了，键一定存在，再怀疑就骂你
        self.只艾特触发等待 = bool(config['只艾特触发等待'])
        self.自动等待秒数 = float(config['自动等待秒数']) # 类型异常直接退出罢工
        self.等待提示语 = dict(config['等待提示语'])
        self.等待开头语:str = self.等待提示语['等待开头语'].strip()
        self.等待末尾语 = self.等待提示语['等待末尾语'].strip()
        self.最大缓存条数 = int(config['最大缓存条数'])
        self.只收集本人 = config['自动收集范围'].strip() == "发送者本人"
        self.未触发规则也收集 = bool(config['未触发规则也收集'])
        self.收集时间差 = float(config['收集时间差'])
        self.已编译正则列表 = []
        self.编译正则()
        self.消息缓存: defaultdict[str, list[AstrMessageEvent]] = defaultdict(list)
        self.会话等待中事件:dict[str, AstrMessageEvent | None] = {}
        self.会话锁: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.会话有新唤醒:dict[str, bool] = {}
        self.同用户循环等待 = bool(config['同用户循环等待'])
        self.循环等待最大时间 = float(config['循环等待最大时间'])

    def 编译正则(self):
        """预编译正则列表"""
        self.已编译正则列表 = []
        for 模式 in self.自动触发正则列表 or []:
            模式 = str(模式 or "").strip()
            if not 模式:
                continue
            try:
                self.已编译正则列表.append(re.compile(模式))
            except re.error as e:
                logger.warning(f"[消息补获] 正则编译失败: {模式!r}: {e}")

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE, priority=sys.maxsize - 2)  # priority数字越大越先执行
    async def 接收消息时(self, event: AstrMessageEvent):
        """接收到消息时，缓存群聊的消息"""
        if not event.get_group_id():
            return
        if not (xxl:=event.get_messages()):
            return
        if isinstance(xxl[0], Poke): # 跳过戳一戳事件
            return
        if event.get_self_id() == event.get_sender_id():
            return
        umo = event.unified_msg_origin
        async with self.会话锁[umo]:
            l = self.消息缓存[umo]
            m = self.最大缓存条数
            l.append(event)
            cd = len(l)
            if cd > m+20:
                del l[:cd-m]
                # 如果长度=24，最大条数=20，那么24-20=4，del[:4]就是删除0,1,2,3，总共4个，剩下20个，没有问题

    @filter.on_waiting_llm_request()
    async def llm队列时(self, event: AstrMessageEvent):
        """进入llm请求队列时，检查是否在等待"""
        if not event.get_group_id():
            return
        umo = event.unified_msg_origin
        if w_event:=self.会话等待中事件.get(umo):
            if w_event.get_sender_id() == event.get_sender_id():
                event.stop_event()  # 同一个人，则直接终止这个事件，由追加机制往等待中的那个事件填补
                event.set_extra(会话主动停止键, True)
                return
        # 不同人唤醒：标记为“下一轮”，并打断当前等待
        event.set_extra(会话已唤醒键, True)
        self.会话有新唤醒[umo] = True

    @filter.on_llm_request()
    async def llm请求前(self, event: AstrMessageEvent, req: ProviderRequest):
        """即将请求llm时，检查是否需要等待"""
        if not event.get_group_id():
            return

        # 理论上此时不会再有正在等待中的同会话的事件，因为llm请求前的事件，已经在事件线上了，
        # 已经从llm队列时取出来了了，如果是另一个会话的，那么此方法会照常同时触发两个会话的等待，互不影响

        if self.会话等待中事件.get(event.unified_msg_origin):
            return  # 如果再判断一次，应该不会冲突

        消息文本 = event.message_str
        消息链 = event.get_messages()
        if (
                (self.只艾特触发等待 and
                (len(消息链) == 1 and isinstance(消息链[0], At) and str(消息链[0].qq or "") == event.get_self_id()))
                or (self.已编译正则列表 and any(p.search(消息文本) for p in self.已编译正则列表))
        ):
            等待秒数 = self.自动等待秒数
        elif self.未触发规则也收集 and (time.time() - event.message_obj.timestamp > self.收集时间差):
            等待秒数 = 0
        else:
            return
        # 触发等待
        if self.同用户循环等待:
            片段 = await self.循环等待(event, 等待秒数=等待秒数)
        else:
            片段 = await self.等待收集新消息(event, 等待秒数=等待秒数)
        if not 片段:
            return
        消息数 = len(片段)
        if self.等待开头语:
            片段.insert(0,
                        self.等待开头语
                        .replace("{等待时间}", f"{(time.time() - event.message_obj.timestamp):.1f}")
                        .replace("{消息数量}", str(消息数))
                        )
        if self.等待末尾语:
            片段.append(self.等待末尾语)

        最终文本 = '\n'.join(片段)

        req.extra_user_content_parts.append(TextPart(text=最终文本))

    async def 等待收集新消息(
            self,
            event: AstrMessageEvent,
            等待秒数:int | float | None = None,
            只收集本人:bool | None= None,
            循环收集:bool = False
    ) -> list[str] | tuple[bool, list[str]]:
        """核心方法，等待新消息
        如果循环收集=True，那么会返回两个值，否则原样返回一个值，不影响旧调用"""
        if 等待秒数 is None:
            等待秒数 = self.自动等待秒数
        if 只收集本人 is None:
            只收集本人 = self.只收集本人
        umo = event.unified_msg_origin
        self_id = event.get_self_id()
        self.会话等待中事件[umo] = event
        self.会话有新唤醒[umo] = False
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 等待秒数
        interval = 0.1
        if 等待秒数:
            while True:  # 切片等待，一有新唤醒则停止，防止把新唤醒拖住
                if self.会话有新唤醒.get(umo):
                    break
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                # 最后一片可能不足 0.1 秒，取较小值，不超时
                await asyncio.sleep(min(interval, remaining))

        def 格式化事件(event_: AstrMessageEvent):
            """将事件详情格式化为可读的字符串"""
            消息链 = event_.get_messages()

            def 遍历消息链(消息链_) -> str:
                p = []
                for 组件 in 消息链_:
                    if isinstance(组件, Reply):
                        p.append(f" [引用了{组件.sender_nickname}({组件.sender_id})的消息] {遍历消息链(组件.chain or [])} ")
                    elif isinstance(组件, Plain):
                        p.append(str(组件.text or ""))
                    elif isinstance(组件, At):
                        qq = str(组件.qq or "")
                        if qq == self_id:
                            p.append(f" @{组件.name}(yourself) ")
                        else:
                            p.append(f" @{组件.name}({qq}) ")
                    elif isinstance(组件, Image):
                        p.append(f" [图片或表情: {组件.url}] ")
                    ...
                return "".join(p)

            return 遍历消息链(消息链)

        def 格式化时间(时间戳: int | float) -> str:
            """把时间戳格式化为北京时间可读的时:分:秒"""
            return datetime.fromtimestamp(时间戳, tz=北京时间).strftime("%H:%M:%S")

        async with self.会话锁[umo]:
            try:
                最后事件位置 = -1
                if not (会话事件列表 := self.消息缓存.get(umo)):
                    if 循环收集:
                        return False, []  # 期间没有收到新消息
                    else:
                        return []
                片段 = []
                # 查找本次事件在列表里的位置
                本次事件位置 = -1
                for idx, p_event in enumerate(会话事件列表):
                    if p_event is event:
                        本次事件位置 = idx
                        break
                最后事件位置 = 本次事件位置 # 找到就用该索引+1遍历，找不到就相当于已经全是新事件了，-1仍然正确
                需要继续 = False
                for p_event in 会话事件列表[本次事件位置+1:]:
                    if p_event.get_extra(会话已唤醒键):
                        break  # 已唤醒及之后的消息都会在下一轮llm处理
                    最后事件位置 += 1 # 要删除的已使用的事件的位置，这个赋值后的值就是p_event在列表里的索引位置
                    if p_event.is_stopped():
                        if p_event.get_extra(会话主动停止键, False):
                            # 这是被本插件合并掉的事件：照常收集，遍历结束后再决定是否继续循环等待
                            需要继续 = True
                        else:
                            continue
                    if p_event.call_llm or getattr(p_event, "_has_send_oper", False):
                        # event.call_llm的语义被写反了，实际上True是禁止请求llm
                        continue  # 事件已被终止，已被设置禁用llm请求，已发送过消息，这些情况按框架预期都不会请求llm
                    if 只收集本人 and (p_event.get_sender_id() != event.get_sender_id()):
                        continue
                    片段.append(f"[{格式化时间(p_event.message_obj.timestamp)}"
                                f" {p_event.get_sender_name()}({p_event.get_sender_id()})]"
                                f" {格式化事件(p_event)}")
                if 循环收集:
                    return 需要继续, 片段
                else:
                    return 片段
            finally:
                if not 循环收集:
                    self.会话等待中事件[umo] = None
                if 最后事件位置 != -1:
                    del self.消息缓存[umo][:最后事件位置+1] # +1包括最后事件本身

    async def 循环等待(
            self,
            event: AstrMessageEvent,
            等待秒数: int | float | None = None,
            只收集本人: bool | None = None,
            最大循环时间:int | None = None
            ):
        """循环触发等待，适用于一直都是同一个用户连续唤醒"""
        片段收集 = []
        if 最大循环时间 is None:
            最大循环时间 = self.循环等待最大时间
        开始时间 = time.time()
        try:
            while True:
                需要继续, 片段 = await self.等待收集新消息(event, 等待秒数, 只收集本人, True)
                片段收集.extend(片段)
                if not 需要继续:
                    break
                if 最大循环时间 == -1:
                    continue
                if time.time() - 开始时间 > 最大循环时间:
                    break
        finally:
            self.会话等待中事件[event.unified_msg_origin] = None
        return 片段收集

    @filter.llm_tool(name="wait_new_messages")
    async def wait_new_messages(
            self,
            event: AstrMessageEvent,
            seconds: float | None = None,
            scope: str | None = None
    ) -> str:
        """等待一小段时间，收集群友在你思考/处理期间又发来的消息。
        适用场景：群聊，群友艾特你之后，问题还没问，或只问你在吗，可能即将有需要补充的消息，但是那条消息如果未艾特你的话，并不会被唤醒
        例如：
            @bot 问你一个问题 → 此条唤醒了
            今天天气怎么样 → 此条没有艾特，不会触发唤醒，落空

        Args:
            seconds(number): 等待秒数，通常2秒，0~8秒，默认根据配置
            scope(string): 收集范围，"user" 只收集当前用户的消息（默认），"all" 收集本群所有人的消息，默认根据插件配置
        """
        已使用次数 = event.get_extra(已使用工具等待键, 0)
        if 已使用次数 >= 2:
            return "你已使用工具超过两次，无法继续再使用了"
        if seconds is None:
            seconds = self.自动等待秒数
        try:
            等待秒数 = float(seconds)
        except (ValueError, TypeError):
            return "无法转换参数seconds的值为浮点数，请检查"
        if not (0 <= 等待秒数 <= 8):  # 0秒也可以，即从请求llm到使用工具期间
            return "seconds等待秒数超出范围"
        scope = str(scope or "").strip().lower()
        if scope:
            只收集本人 = self.只收集本人
        elif scope == "user":
            只收集本人 = True
        elif scope == "all":
            只收集本人 = False
        else:
            return f"参数scope不支持该值：{scope}，支持的值有：user、all或留空"
        event.set_extra(已使用工具等待键, 已使用次数 + 1)
        return '\n'.join(await self.等待收集新消息(event, 等待秒数=等待秒数, 只收集本人=只收集本人)) or "没有收集到新消息"