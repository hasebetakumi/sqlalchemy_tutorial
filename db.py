from sqlalchemy import create_engine


# 特定の文字列への接続の中心
# 特定のdbサーバーのために一度だけ作成されるグローバルオブジェクト
# sqlite→どのような種類のdbと接続するか
# pysqlite→どのようなDBAPIを用いるか→省略したらデフォルトを使う
# /:memory:→DBの場所、今回のフレーズはメモリ内のDBを使用する宣言
# sqlite:///blog.db→動画のやつで指定してた文字列
# future=True→2.0をフル活用するためのもの
# echo=True→sql文を全て表示する
# 以下を実行しても接続を試みたわけではない→DBへのタスク実行時に初めて接続する
engine = create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)

# 以下はあまり使わないがtext()を用いてsql文を記載できる
# 現代ではsession.execute()を用いる。以下はconnection.execute()
# 変更の反映にはcommitが必要
from sqlalchemy import text
with engine.connect() as conn:
    conn.execute(text("CREATE TABLE some_table (x int, y int)"))
    conn.execute(
        text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
        [{"x": 1, "y": 1}, {"x": 2, "y": 4}],
    )
    conn.commit()

# engine.begin()→成功した場合は自動でcommit、失敗した場合はrollbackを返す
with engine.begin() as conn:
    conn.execute(
        text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
        [{"x": 6, "y": 8}, {"x": 9, "y": 10}],
    )

# resultに代入（iterable）して表示
with engine.connect() as conn:
    result = conn.execute(text("SELECT x, y FROM some_table"))
    # →for文で展開して表示する
    for row in result:
        print(f"x: {row.x}  y: {row.y}")

    # 中身をそれぞれ引き抜くこともできる
    for x, y in result:
        print(f"x: {x}  y: {y}")

    # インデックス番号で引き抜くこともできる
    for row in result:
        x = row[0]

    # mapで引き抜くこともできる
    for dict_row in result.mappings():
        x = dict_row["x"]
        y = dict_row["y"]


# SQLに変数を入れたい場合はexecuteの第2引数に入れる
# →インジェクション攻撃対策のため、直接sql文に入れることはしない
with engine.connect() as conn:
    result = conn.execute(text("SELECT x, y FROM some_table WHERE y > :y"), {"y": 2})
    for row in result:
        print(f"x: {row.x}  y: {row.y}")


# 複数のinsertの実行と複数のパラメータ
with engine.connect() as conn:
    conn.execute(
        text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
        [{"x": 11, "y": 12}, {"x": 13, "y": 14}],
    )
    conn.commit()



"""
以下からがORM!!!
"""
# Session→ORM を使うときの基本的なトランザクション／データベース対話型オブジェクト
# connectionと非常によく似た使い方をする
# connとしてのwith engine.connect()の呼び出しをsessionとしてのwith Session(engine)に直接置き換え、
# Connection.execute()メソッドと同じようにSession.execute()メソッドを使用
from sqlalchemy.orm import Session

stmt = text("SELECT x, y FROM some_table WHERE y > :y ORDER BY x, y")
with Session(engine) as session:
    result = session.execute(stmt, {"y": 6})
    for row in result:
        print(f"x: {row.x}  y: {row.y}")


# アプリケーション全体で単一のMetadataオブジェクトを持つのが一般的
# 複数あってもいいが、互いに関連する一連のTableオブジェクトが単一のMetadataオブジェクトに属しているのが望ましい
from sqlalchemy import MetaData
metadata_obj = MetaData()


from sqlalchemy import Table, Column, Integer, String
user_table = Table(
    "user_account", # テーブル名
    metadata_obj, # どのMetadataオブジェクトに割り当てるか
    Column("id", Integer, primary_key=True),
    Column("name", String(30)),
    Column("fullname", String),
)

# 上記テーブルへのアクセス法
# user_table.c.name
# ColimnオブジェクトにはTable.cでアクセス


# テーブル同士のリレーション方法
from sqlalchemy import ForeignKey
address_table = Table(
    "address",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    # ForeignKey→リレーション作成
    # ForeignKeyを利用した場合は型を省略できる
    Column("user_id", ForeignKey("user_account.id"), nullable=False),
    Column("email_address", String, nullable=False),
)

# テーブル作成
# 以下は自動でcommitされる
# リレーションでミスがないようにcreateする順番は考慮されている
metadata_obj.create_all(engine)



"""
上記のデータ構造と同じものをよりORM中心の構成方法で宣言する例
"""
# 以下を実行すると、自動的にTableオブジェクトのコレクションを格納するMetadataオブジェクトが含まれるようになる。
# MetaDataコレクションはORM専用のregistryオブジェクトに含まれている
from sqlalchemy.orm import registry
mapper_registry = registry()
Base = mapper_registry.generate_base()

# 上の3行は以下でまとめられる
# !!!declarativeを使用すると、クラス定義が自動的にテーブル定義とひもづく!!!
from sqlalchemy.orm import declarative_base
Base = declarative_base()


# Tableオブジェクトを使用せず、マップされたクラスに間接的に宣言
from sqlalchemy.orm import relationship
# Baseを引き継いでクラス作成
class User(Base):
    __tablename__ = "user_account"

    id = Column(Integer, primary_key=True)
    name = Column(String(30))
    fullname = Column(String)

    # 以下のrelationshipはテーブル構造の定義に必須なわけではないがリレーションさせたいので宣言
    addresses = relationship("Address", back_populates="user")

    def __repr__(self):
        return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"

# Baseを引き継いでクラス作成
class Address(Base):
    __tablename__ = "address"

    id = Column(Integer, primary_key=True)
    email_address = Column(String, nullable=False)
    user_id = Column(Integer, ForeignKey("user_account.id"))

    # 以下のrelationshipはテーブル構造の定義に必須なわけではないがリレーションさせたいので宣言
    user = relationship("User", back_populates="addresses")

    def __repr__(self):
        return f"Address(id={self.id!r}, email_address={self.email_address!r})"

"""
以下でテーブル作成
"""
# emit CREATE statements given ORM registry
mapper_registry.metadata.create_all(engine)

# the identical MetaData object is also present on the
# declarative base
Base.metadata.create_all(engine)


# Tableオブジェクトとそれぞれのクラスを生成する方法を混ぜることもできる
mapper_registry = registry()
Base = mapper_registry.generate_base()


class User(Base):
    # ここでTableオブジェクトを代入
    __table__ = user_table

    addresses = relationship("Address", back_populates="user")

    def __repr__(self):
        return f"User({self.name!r}, {self.fullname!r})"


class Address(Base):
    # ここでTableオブジェクトを代入
    __table__ = address_table

    user = relationship("User", back_populates="addresses")

    def __repr__(self):
        return f"Address({self.email_address!r})"


# テーブルの使用方法
# テーブル名とテーブルが属するMetadataを指定して取り出す
some_table = Table("some_table", metadata_obj, autoload_with=engine)


"""
以下からデータの操作（CRUD）
"""
# データ挿入
# 以下が最も簡単なinsert方法
from sqlalchemy import insert
# stmt内には文字列となったSQL文が格納される
stmt = insert(user_table).values(name="spongebob", fullname="Spongebob Squarepants")

# SQL文の実行
with engine.connect() as conn:
    result = conn.execute(stmt)
    conn.commit()


# 同じテーブルに複数insert
with engine.connect() as conn:
    result = conn.execute(
        insert(user_table),
        [
            {"name": "sandy", "fullname": "Sandy Cheeks"},
            {"name": "patrick", "fullname": "Patrick Star"},
        ],
    )
    conn.commit()

"""
select
"""
from sqlalchemy import select
# Tableオブジェクトで指定
print(select(user_table))
# SELECT user_account.id, user_account.name, user_account.fullname
# FROM user_account

# クラスで指定
print(select(User))
# SELECT user_account.name, user_account.fullname
# FROM user_account

# session.executeで指定すると以下のように取得
row = session.execute(select(User)).first()
# row
# (User(id=1, name='spongebob', fullname='Spongebob Squarepants'),)
# row[0]
# User(id=1, name='spongebob', fullname='Spongebob Squarepants')


# session.scalarsで指定すると以下のように取得
user = session.scalars(select(User)).first()
# user
# User(id=1, name='spongebob', fullname='Spongebob Squarepants')

"""
!!!以下からがORMのデータ操作
"""
# create
# この段階ではidはまだNone
squidward = User(name="squidward", fullname="Squidward Tentacles")

# エンジンを指定してセッションオブジェクト作成
# ！！！あとで必ずsessionを閉じる必要がある
# →Session.commit()、Session.rollback()、Session.close()メソッドのいずれかを呼び出すまで、開いたまま
session = Session(engine)

# insert
session.add(squidward)

# 反映
session.commit()

# 保留中→commit前のオブジェクトはsession.newで見ることができる
# session.new
# IdentitySet([User(id=None, name='squidward', fullname='Squidward Tentacles')])

# commitしたときにどんなsqlが発行されるかはsession.flushで見ることができる
# 基本はcommitしたときに自動フラッシュされる
# session.flush()
# BEGIN (implicit)
# INSERT INTO user_account (name, fullname) VALUES (?, ?)
# [...] ('squidward', 'Squidward Tentacles')




# select
# テーブルクラスと主キーを指定してselect
some_squidward = session.get(User, 4)

# update
# 拾ってきたやつをそのまま変える
some_squidward.fullname = "Sandy Squirrel"
session.commit()

# delete
# 拾ってきたやつをdelete
session.delete(some_squidward)
session.commit()


# rollback
# 現在seesionに関連づけられている変更を削除する
# !tryに失敗した時とかに使う
session.rollback()

# withを使わなかったら明示的にcloseするのが望ましい
# commitされなかった変更は全てロールバックされる
# sessionが関与するオブジェクトも全て削除される
session.close()


"""
リレーションの操作
以下は上でも書いたテーブル

# Baseを引き継いでクラス作成
class User(Base):
    __tablename__ = "user_account"

    id = Column(Integer, primary_key=True)
    name = Column(String(30))
    fullname = Column(String)

    # 以下のrelationshipはテーブル構造の定義に必須なわけではないがリレーションさせたいので宣言
    addresses = relationship("Address", back_populates="user")

    def __repr__(self):
        return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"

# Baseを引き継いでクラス作成
class Address(Base):
    __tablename__ = "address"

    id = Column(Integer, primary_key=True)
    email_address = Column(String, nullable=False)
    user_id = Column(Integer, ForeignKey("user_account.id"))

    # 以下のrelationshipはテーブル構造の定義に必須なわけではないがリレーションさせたいので宣言
    user = relationship("User", back_populates="addresses")

    def __repr__(self):
        return f"Address(id={self.id!r}, email_address={self.email_address!r})"
"""

# 上記のようにuser_accountとaddressは1対多の関係
# 多のaddressの複数形にアクセスできるようになる
u1 = User(name="pkrabs", fullname="Pearl Krabs")
# u1.addresses
# []

# 配列が入っているため、u1.addresses.append()で要素の追加もできる
a1 = Address(email_address="pearl.krabs@gmail.com")
u1.addresses.append(a1)

# 1側を取得するときは単数形で良い
# a1.user
# User(id=None, name='pkrabs', fullname='Pearl Krabs')


# 上記の変更をcreateした場合はu1もa1もinsertを待っている状態→sessionが持っている
# →二つの変更はまだ保留中
# →u1のid、a1のid、a1のuser_idはまだNone
# session.add(u1)
# u1 in session
# True
# a1 in session
# True

# !!!正しい順序で自動的にinsertされる!!!
session.commit()
# INSERT INTO user_account (name, fullname) VALUES (?, ?)
# [...] ('pkrabs', 'Pearl Krabs')
# INSERT INTO address (email_address, user_id) VALUES (?, ?)
# [...] ('pearl.krabs@gmail.com', 6)
# COMMIT


