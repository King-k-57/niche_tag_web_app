import os
import re
import unicodedata
from datetime import datetime
from functools import wraps
from urllib.parse import urljoin, urlparse

from flask import Flask, flash, jsonify, redirect, render_template, request, url_for
from flask import session
from flask_login import LoginManager, UserMixin, current_user, login_required, login_user, logout_user
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from flask_wtf.csrf import CSRFError, CSRFProtect
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.security import check_password_hash, generate_password_hash

# Flaskアプリ全体で使うDBインスタンス
db = SQLAlchemy()
migrate = Migrate()
login_manager = LoginManager()
csrf = CSRFProtect()

ALLOWED_CATEGORIES = ("anime", "drama")
CATEGORY_OPTIONS = [
    {"value": "anime", "label": "アニメ"},
    {"value": "drama", "label": "ドラマ"},
]


# 多対多を表す中間テーブル（モデルではなくTableとして定義）
work_tag = db.Table(
    "work_tag",
    db.Column("work_id", db.Integer, db.ForeignKey("works.id"), primary_key=True),
    db.Column("tag_id", db.Integer, db.ForeignKey("tags.id"), primary_key=True),
)

bookmarks = db.Table(
    "bookmarks",
    db.Column("user_id", db.Integer, db.ForeignKey("users.id"), primary_key=True),
    db.Column("work_id", db.Integer, db.ForeignKey("works.id"), primary_key=True),
    db.Column("created_at", db.DateTime, nullable=False, default=datetime.utcnow),
)


class Work(db.Model):
    """作品モデル: MVPではタイトルのみ保持する。"""

    __tablename__ = "works"

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    category = db.Column(db.String(20), nullable=False, default="anime")
    synopsis = db.Column(db.Text, nullable=True)

    # secondaryで中間テーブルを指定し、Tagと多対多で結ぶ
    tags = db.relationship("Tag", secondary=work_tag, back_populates="works", lazy="selectin")
    bookmarked_by = db.relationship("User", secondary=bookmarks, back_populates="bookmarked_works", lazy="selectin")


class Tag(db.Model):
    """ユーザーが自由入力するニッチタグ。"""

    __tablename__ = "tags"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False, unique=True)

    works = db.relationship("Work", secondary=work_tag, back_populates="tags", lazy="selectin")


class User(UserMixin, db.Model):
    """公開運用時の編集権限を管理するユーザーモデル。"""

    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), nullable=False, unique=True)
    password_hash = db.Column(db.String(255), nullable=False)
    is_admin = db.Column(db.Boolean, nullable=False, default=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    bookmarked_works = db.relationship("Work", secondary=bookmarks, back_populates="bookmarked_by", lazy="selectin")

    def set_password(self, raw_password: str) -> None:
        """平文を保存せず、安全なハッシュのみをDBへ保持する。"""
        self.password_hash = generate_password_hash(raw_password)

    def check_password(self, raw_password: str) -> bool:
        """ログイン時の照合をハッシュ比較で行う。"""
        return check_password_hash(self.password_hash, raw_password)


def normalize_tag_name(raw_name: str) -> str:
    """タグ名を正規化して、全角/半角ゆれと余分な空白を吸収する。"""
    # NFKCで「Ａ」「a」「ｱ」のような表記ゆれをできる範囲で統一する
    normalized = unicodedata.normalize("NFKC", raw_name or "")
    # split/joinで連続空白を1つに圧縮し、先頭末尾の空白も除去する
    return " ".join(normalized.split())


def normalize_search_text(raw_text: str) -> str:
    """検索入力の表記ゆれを吸収して検索精度を上げる。"""
    normalized = unicodedata.normalize("NFKC", raw_text or "")
    return " ".join(normalized.split())


def normalize_category(raw_category: str | None) -> str:
    """カテゴリ入力値を正規化し、許可値のみ受け付ける。"""
    normalized = normalize_search_text(raw_category or "").lower()
    if normalized in ALLOWED_CATEGORIES:
        return normalized

    return "anime"


def normalize_synopsis(raw_synopsis: str | None) -> str | None:
    """あらすじ入力を正規化し、空値はNoneとして扱う。"""
    normalized = unicodedata.normalize("NFKC", raw_synopsis or "")
    normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
    compact = "\n".join(line.rstrip() for line in normalized.split("\n")).strip()
    return compact or None


def has_user_bookmarked(work_id: int, user_id: int) -> bool:
    """指定ユーザーが指定作品をブックマーク済みかを判定する。"""
    return (
        db.session.query(bookmarks.c.work_id)
        .filter(bookmarks.c.user_id == user_id, bookmarks.c.work_id == work_id)
        .first()
        is not None
    )

def parse_tag_id_values(raw_values: list[str]) -> list[int]:
    """文字列配列から有効なタグIDだけを重複なしで取り出す。"""
    parsed_ids: list[int] = []
    for raw_value in raw_values:
        try:
            tag_id = int(raw_value)
        except (TypeError, ValueError):
            continue

        if tag_id not in parsed_ids:
            parsed_ids.append(tag_id)

    return parsed_ids

def parse_tags_input(raw_tags: str) -> list[str]:
    """カンマまたは空白区切りのタグ文字列を正規化し、重複を除いて返す。"""
    normalized = unicodedata.normalize("NFKC", raw_tags or "")
    # 日本語カンマも同様に扱えるように統一する
    normalized = normalized.replace("、", ",")

    # カンマと空白の両方を区切り文字として分割する
    candidates = re.split(r"[,\s]+", normalized)

    parsed_tags: list[str] = []
    seen: set[str] = set()

    for candidate in candidates:
        tag_name = normalize_tag_name(candidate)
        if not tag_name or tag_name in seen:
            continue
        seen.add(tag_name)
        parsed_tags.append(tag_name)

    return parsed_tags


def cleanup_orphan_tags() -> int:
    """どの作品にも紐づいていないタグを削除し、件数を返す。"""
    orphan_tags = (
        Tag.query.outerjoin(work_tag, Tag.id == work_tag.c.tag_id)
        .outerjoin(Work, Work.id == work_tag.c.work_id)
        .group_by(Tag.id)
        .having(db.func.count(Work.id) == 0)
        .all()
    )

    for orphan in orphan_tags:
        db.session.delete(orphan)

    if orphan_tags:
        db.session.commit()

    return len(orphan_tags)


def format_safe_error_message(error: Exception) -> str:
    """画面表示向けに、内部情報を出しすぎないエラー要約を返す。"""
    # 生の例外全文をそのまま表示すると内部情報が漏れる可能性があるため、
    # 例外クラス名を中心に短い要約だけを返す。
    error_name = error.__class__.__name__
    raw_message = " ".join(str(error).split())

    if not raw_message:
        return error_name

    return f"{error_name}: {raw_message[:120]}"


def is_safe_next_url(next_url: str | None) -> bool:
    """ログイン後リダイレクト先が同一オリジンかを検証してOpen Redirectを防ぐ。"""
    if not next_url:
        return False

    host_url = urlparse(request.host_url)
    redirect_url = urlparse(urljoin(request.host_url, next_url))
    return redirect_url.scheme in {"http", "https"} and host_url.netloc == redirect_url.netloc


def admin_required(view_func):
    """管理者だけが実行できる操作に付与するデコレータ。"""

    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if not current_user.is_authenticated:
            if request.path.startswith("/api/"):
                return jsonify({"ok": False, "message": "認証が必要です。ログインしてください。"}), 401

            next_url = request.full_path if request.query_string else request.path
            return redirect(url_for("login", next=next_url))

        if not getattr(current_user, "is_admin", False):
            if request.path.startswith("/api/"):
                return jsonify({"ok": False, "message": "この操作は管理者のみ実行できます。"}), 403

            flash("この操作は管理者のみ実行できます。", "warning")
            requested_category = normalize_category(request.values.get("category"))
            return redirect(url_for("index", category=requested_category, filter_applied=1))

        return view_func(*args, **kwargs)

    return wrapped


def create_app() -> Flask:
    app = Flask(__name__)

    # SQLiteファイルをinstance配下に固定して環境差を減らす
    base_dir = os.path.abspath(os.path.dirname(__file__))
    instance_dir = os.path.join(base_dir, "instance")
    os.makedirs(instance_dir, exist_ok=True)

    app.config["SECRET_KEY"] = "dev-secret-key"

    # 本番ではDATABASE_URL(PostgreSQL)を優先し、未設定時のみローカルSQLiteを使う。
    database_url = os.getenv("DATABASE_URL", "").strip()
    if database_url:
        # Renderなどで返るpostgres://形式をSQLAlchemy互換のpostgresql://へ補正する。
        if database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql://", 1)
        app.config["SQLALCHEMY_DATABASE_URI"] = database_url
    else:
        app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{os.path.join(instance_dir, 'app.db')}"

    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    db.init_app(app)
    migrate.init_app(app, db)
    login_manager.init_app(app)
    csrf.init_app(app)
    login_manager.login_view = "login"
    login_manager.login_message = "編集操作にはログインが必要です。"
    login_manager.login_message_category = "info"

    @app.errorhandler(CSRFError)
    def handle_csrf_error(error: CSRFError):
        """CSRF検証失敗時に、画面/APIそれぞれへ分かりやすく応答する。"""
        if request.path.startswith("/api/"):
            return (
                jsonify(
                    {
                        "ok": False,
                        "message": "CSRFトークンが無効です。ページを再読み込みして再試行してください。",
                        "error": format_safe_error_message(error),
                    }
                ),
                400,
            )

        flash("フォームの有効期限が切れたか、不正なリクエストです。再度お試しください。", "warning")
        return redirect(request.referrer or url_for("index", filter_applied=1))

    @login_manager.user_loader
    def load_user(user_id: str):
        """セッション内のユーザーIDからログインユーザーを復元する。"""
        try:
            return db.session.get(User, int(user_id))
        except (TypeError, ValueError):
            return None

    @login_manager.unauthorized_handler
    def unauthorized():
        """未認証アクセス時の応答を画面/APIで出し分ける。"""
        if request.path.startswith("/api/"):
            return jsonify({"ok": False, "message": "認証が必要です。ログインしてください。"}), 401

        next_url = request.full_path if request.query_string else request.path
        return redirect(url_for("login", next=next_url))

    # テーブル作成や変更はFlask-Migrate経由で管理する
    def get_active_filter_ids() -> list[int]:
        """sessionに保持されたアクティブなタグIDを取得する。"""
        stored = session.get("active_tag_ids", [])
        if not isinstance(stored, list):
            return []

        return parse_tag_id_values([str(value) for value in stored])

    def set_active_filter_ids(tag_ids: list[int]) -> None:
        """アクティブフィルター状態をsessionへ保存する。"""
        session["active_tag_ids"] = tag_ids

    @app.context_processor
    def inject_active_filter_context():
        """base.htmlから常に参照できる選択中フィルター情報を提供する。"""
        active_ids = get_active_filter_ids()
        current_category = normalize_category(request.args.get("category"))

        if not active_ids:
            return {
                "active_filter_tag_ids": [],
                "active_filter_tags": [],
                "current_category": current_category,
                "category_options": CATEGORY_OPTIONS,
            }

        active_tags = Tag.query.filter(Tag.id.in_(active_ids)).order_by(Tag.name.asc()).all()
        normalized_ids = [tag.id for tag in active_tags]

        # 既に削除されたタグIDがsessionに残っていた場合に整合性を取る
        if normalized_ids != active_ids:
            set_active_filter_ids(normalized_ids)

        return {
            "active_filter_tag_ids": normalized_ids,
            "active_filter_tags": active_tags,
            "current_category": current_category,
            "category_options": CATEGORY_OPTIONS,
        }

    @app.get("/")
    def index():
        """トップページ: 同時登録フォームと複数タグAND検索を表示する。"""
        # 大規模データでも表示負荷を抑えるため、一覧表示はページ単位で取得する。
        page = request.args.get("page", default=1, type=int)
        per_page = 20
        current_category = normalize_category(request.args.get("category"))

        tag_catalog_rows = (
            db.session.query(Tag, db.func.count(db.distinct(Work.id)).label("work_count"))
            .outerjoin(work_tag, Tag.id == work_tag.c.tag_id)
            .outerjoin(Work, Work.id == work_tag.c.work_id)
            .filter((Work.category == current_category) | (Work.id.is_(None)))
            .group_by(Tag.id)
            .order_by(Tag.name.asc())
            .all()
        )
        tag_catalog = [{"tag": row[0], "work_count": row[1]} for row in tag_catalog_rows if row[1] > 0]

        popular_tag_rows = (
            db.session.query(Tag, db.func.count(db.distinct(Work.id)).label("work_count"))
            .join(work_tag, Tag.id == work_tag.c.tag_id)
            .join(Work, Work.id == work_tag.c.work_id)
            .filter(Work.category == current_category)
            .group_by(Tag.id)
            .order_by(db.func.count(db.distinct(Work.id)).desc(), Tag.name.asc())
            .limit(24)
            .all()
        )
        popular_tags = [{"tag": row[0], "work_count": row[1]} for row in popular_tag_rows]

        # AND検索フォームが送信された場合は、URLパラメータをsessionへ反映する
        filter_applied = request.args.get("filter_applied")
        if filter_applied is not None:
            selected_tag_ids = parse_tag_id_values(request.args.getlist("tag_ids"))
            set_active_filter_ids(selected_tag_ids)
        else:
            selected_tag_ids = get_active_filter_ids()

        recent_works = []
        works_pagination = None
        selected_tags = []
        and_results = None
        and_results_pagination = None
        if selected_tag_ids:
            selected_tags = Tag.query.filter(Tag.id.in_(selected_tag_ids)).order_by(Tag.name.asc()).all()
            selected_tag_ids = [tag.id for tag in selected_tags]
            set_active_filter_ids(selected_tag_ids)

            if selected_tag_ids:
                # AND検索: 選択タグに一致する作品を集約し、件数が選択数と一致するものだけ返す
                and_query = (
                    Work.query.join(Work.tags)
                    .filter(Tag.id.in_(selected_tag_ids), Work.category == current_category)
                    .group_by(Work.id)
                    .having(db.func.count(db.distinct(Tag.id)) == len(selected_tag_ids))
                    .order_by(Work.id.desc())
                )
                and_results_pagination = and_query.paginate(page=page, per_page=per_page, error_out=False)
                and_results = and_results_pagination.items
            else:
                and_results = []
        else:
            works_pagination = Work.query.filter(Work.category == current_category).order_by(Work.id.desc()).paginate(
                page=page,
                per_page=per_page,
                error_out=False,
            )
            recent_works = works_pagination.items

        return render_template(
            "index.html",
            recent_works=recent_works,
            works_pagination=works_pagination,
            tag_catalog=tag_catalog,
            selected_tag_ids=selected_tag_ids,
            selected_tags=selected_tags,
            and_results=and_results,
            and_results_pagination=and_results_pagination,
            current_category=current_category,
            popular_tags=popular_tags,
        )

    @app.route("/register", methods=["GET", "POST"])
    def register():
        """一般公開向け: 新規ユーザー登録を行う。"""
        if current_user.is_authenticated:
            return redirect(url_for("index", filter_applied=1))

        if request.method == "POST":
            username = normalize_search_text(request.form.get("username", ""))
            password = request.form.get("password", "")
            password_confirm = request.form.get("password_confirm", "")

            if len(username) < 3:
                flash("ユーザー名は3文字以上で入力してください。", "warning")
                return render_template("register.html", form_username=username)

            if len(password) < 8:
                flash("パスワードは8文字以上で入力してください。", "warning")
                return render_template("register.html", form_username=username)

            if password != password_confirm:
                flash("確認用パスワードが一致しません。", "warning")
                return render_template("register.html", form_username=username)

            existing_user = User.query.filter_by(username=username).first()
            if existing_user is not None:
                flash("そのユーザー名は既に使用されています。", "warning")
                return render_template("register.html", form_username=username)

            try:
                user = User(username=username)
                user.set_password(password)
                db.session.add(user)
                db.session.commit()

                # 登録直後にログイン状態へ遷移させ、投稿参加までの離脱を減らす。
                login_user(user)
                flash("アカウントを作成し、ログインしました。", "success")
                return redirect(url_for("index", filter_applied=1))
            except SQLAlchemyError as error:
                db.session.rollback()
                flash("アカウント登録に失敗しました。", "warning")
                flash(f"エラー詳細: {format_safe_error_message(error)}", "info")

        return render_template("register.html", form_username="")

    @app.route("/login", methods=["GET", "POST"])
    def login():
        """一般公開向け: ユーザーログインを行う。"""
        if current_user.is_authenticated:
            return redirect(url_for("index", filter_applied=1))

        next_url = request.args.get("next", "")

        if request.method == "POST":
            username = normalize_search_text(request.form.get("username", ""))
            password = request.form.get("password", "")
            remember_me = request.form.get("remember_me") == "on"

            user = User.query.filter_by(username=username).first()
            if user is None or not user.check_password(password):
                flash("ユーザー名またはパスワードが正しくありません。", "warning")
                return render_template("login.html", next_url=next_url, form_username=username)

            login_user(user, remember=remember_me)
            flash("ログインしました。", "success")

            if is_safe_next_url(next_url):
                return redirect(next_url)

            return redirect(url_for("index", filter_applied=1))

        return render_template("login.html", next_url=next_url, form_username="")

    @app.post("/logout")
    @login_required
    def logout():
        """ログインセッションを終了する。"""
        logout_user()
        flash("ログアウトしました。", "info")
        return redirect(url_for("index", filter_applied=1))

    @app.get("/filters/remove/<int:tag_id>")
    @app.get("/remove-filter/<int:tag_id>")
    def remove_active_filter(tag_id: int):
        """選択中フィルターから特定タグを外し、AND検索結果へ戻す。"""
        current_category = normalize_category(request.args.get("category"))
        active_ids = get_active_filter_ids()
        if tag_id in active_ids:
            active_ids.remove(tag_id)
            set_active_filter_ids(active_ids)

        if active_ids:
            return redirect(url_for("index", category=current_category, tag_ids=active_ids, filter_applied=1))

        return redirect(url_for("index", category=current_category, filter_applied=1))

    @app.get("/filters/clear")
    @app.get("/clear-filters")
    def clear_active_filters():
        """選択中フィルターをすべて解除する。"""
        current_category = normalize_category(request.args.get("category"))
        set_active_filter_ids([])
        return redirect(url_for("index", category=current_category, filter_applied=1))

    @app.post("/works")
    @app.post("/works/create")
    @app.post("/create-work")
    @login_required
    def create_work():
        """トップページから作品を登録し、入力タグを同時にまとめて紐づける。"""
        title = normalize_search_text(request.form.get("title", ""))
        raw_tags = request.form.get("tags", "")
        category = normalize_category(request.form.get("category"))
        synopsis = normalize_synopsis(request.form.get("synopsis", ""))

        if not title:
            flash("作品タイトルを入力してください。", "warning")
            return redirect(url_for("index", category=category, filter_applied=1))

        work = Work(title=title, category=category, synopsis=synopsis)
        db.session.add(work)

        # カンマ/空白区切りのタグを一括で処理する
        parsed_tags = parse_tags_input(raw_tags)
        linked_count = 0

        for tag_name in parsed_tags:
            tag = Tag.query.filter_by(name=tag_name).first()
            if tag is None:
                tag = Tag(name=tag_name)
                db.session.add(tag)

            if tag not in work.tags:
                work.tags.append(tag)
                linked_count += 1

        db.session.commit()

        if linked_count > 0:
            flash(f"作品を登録し、タグを{linked_count}件紐づけました。", "success")
        else:
            flash("作品を登録しました。", "success")

        return redirect(url_for("index", category=category, filter_applied=1))

    @app.post("/works/bulk")
    @login_required
    def bulk_create_works():
        """改行区切りの作品名リストを受け取り、一括で登録する。"""
        raw_bulk_titles = request.form.get("bulk_titles", "")
        category = normalize_category(request.form.get("category"))

        # 改行ごとにタイトルを取り出して正規化する
        normalized_titles = [normalize_search_text(line) for line in raw_bulk_titles.splitlines()]
        candidate_titles = [title for title in normalized_titles if title]

        if not candidate_titles:
            flash("一括登録する作品名を1件以上入力してください。", "warning")
            return redirect(url_for("index", category=category, filter_applied=1))

        try:
            existing_pairs = {(row[0], row[1]) for row in db.session.query(Work.title, Work.category).all()}
            seen_in_request: set[str] = set()
            created_count = 0
            duplicate_count = 0
            failed_items: list[tuple[str, str]] = []

            for title in candidate_titles:
                if title in seen_in_request:
                    duplicate_count += 1
                    continue

                seen_in_request.add(title)

                if (title, category) in existing_pairs:
                    duplicate_count += 1
                    continue

                try:
                    # 1件ずつcommitしておくことで、特定タイトルだけ失敗しても
                    # 他タイトルの登録処理を継続できるようにする。
                    db.session.add(Work(title=title, category=category))
                    db.session.commit()
                    existing_pairs.add((title, category))
                    created_count += 1
                except SQLAlchemyError as error:
                    # DB例外時は必ずrollbackし、セッション破損で次の処理が
                    # 連鎖失敗しないようにする。
                    db.session.rollback()
                    failed_items.append((title, format_safe_error_message(error)))

            if created_count > 0:
                flash(f"作品を{created_count}件一括登録しました。", "success")
            if duplicate_count > 0:
                flash(f"重複タイトルを{duplicate_count}件スキップしました。", "info")

            if failed_items:
                # 失敗タイトルを明示して再実行しやすくする（長文化を防ぐため上限あり）。
                preview_limit = 3
                preview_text = " / ".join(
                    f"{title}（{error_summary}）" for title, error_summary in failed_items[:preview_limit]
                )
                flash(f"登録に失敗した作品: {preview_text}", "warning")

                if len(failed_items) > preview_limit:
                    remain = len(failed_items) - preview_limit
                    flash(f"ほか{remain}件の失敗があります。入力内容を見直して再試行してください。", "info")

            return redirect(url_for("index", category=category, filter_applied=1))
        except Exception as error:
            # 想定外例外でもサーバーを落とさず、次リクエストに備えてrollbackする。
            db.session.rollback()
            flash("一括登録中に予期しないエラーが発生しました。", "warning")
            flash(f"エラー詳細: {format_safe_error_message(error)}", "info")
            return redirect(url_for("index", category=category, filter_applied=1))

    @app.get("/works/search")
    def search_works_by_title():
        """作品タイトルの部分一致検索を行う。"""
        query = normalize_search_text(request.args.get("q", ""))
        current_category = normalize_category(request.args.get("category"))
        page = request.args.get("page", default=1, type=int)
        per_page = 20

        if not query:
            flash("検索する作品名を入力してください。", "warning")
            return redirect(url_for("index", category=current_category, filter_applied=1))

        # LIKE検索で % や _ を入力された場合に意図しないワイルドカード展開を
        # 防ぐため、エスケープして部分一致パターンを組み立てる。
        escaped_query = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        like_pattern = f"%{escaped_query}%"

        result_query = (
            Work.query.filter(Work.title.ilike(like_pattern, escape="\\"), Work.category == current_category)
            .order_by(Work.id.desc())
        )
        pagination = result_query.paginate(page=page, per_page=per_page, error_out=False)
        works = pagination.items

        # UX向上のため、1件ヒット時は結果一覧を挟まず詳細へ直接遷移する。
        if pagination.total == 1:
            only_work = result_query.first()
            if only_work is not None:
                return redirect(url_for("work_detail", work_id=only_work.id))

        return render_template(
            "title_search_results.html",
            query=query,
            works=works,
            pagination=pagination,
            current_category=current_category,
        )

    @app.get("/api/tags/suggest")
    def suggest_tags():
        """タグ入力用の候補をJSONで返す。"""
        query = normalize_search_text(request.args.get("q", ""))
        current_category = normalize_category(request.args.get("category"))
        limit = request.args.get("limit", default=12, type=int)
        limit = max(1, min(limit, 30))

        if not query:
            return jsonify({"ok": True, "items": []})

        escaped_query = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        like_pattern = f"%{escaped_query}%"

        category_rows = (
            db.session.query(Tag, db.func.count(db.distinct(Work.id)).label("work_count"))
            .join(work_tag, Tag.id == work_tag.c.tag_id)
            .join(Work, Work.id == work_tag.c.work_id)
            .filter(Tag.name.ilike(like_pattern, escape="\\"), Work.category == current_category)
            .group_by(Tag.id)
            .order_by(db.func.count(db.distinct(Work.id)).desc(), Tag.name.asc())
            .limit(limit)
            .all()
        )

        if category_rows:
            items = [{"id": row[0].id, "name": row[0].name, "work_count": row[1]} for row in category_rows]
            return jsonify({"ok": True, "items": items})

        fallback_tags = Tag.query.filter(Tag.name.ilike(like_pattern, escape="\\")).order_by(Tag.name.asc()).limit(limit).all()
        items = [{"id": tag.id, "name": tag.name, "work_count": 0} for tag in fallback_tags]
        return jsonify({"ok": True, "items": items})

    @app.get("/work/<int:work_id>")
    def work_detail(work_id: int):
        """作品詳細ページ表示を担当する。"""
        work = db.get_or_404(Work, work_id)
        is_bookmarked = current_user.is_authenticated and has_user_bookmarked(work.id, current_user.id)
        return render_template(
            "work_detail.html",
            work=work,
            current_category=work.category,
            is_bookmarked=is_bookmarked,
        )

    @app.post("/work/<int:work_id>/synopsis")
    @login_required
    def update_work_synopsis(work_id: int):
        """作品のあらすじを更新する。"""
        work = db.get_or_404(Work, work_id)
        synopsis = normalize_synopsis(request.form.get("synopsis", ""))
        work.synopsis = synopsis
        db.session.commit()

        if synopsis:
            flash("あらすじを更新しました。", "success")
        else:
            flash("あらすじを空に更新しました。", "info")

        return redirect(url_for("work_detail", work_id=work.id))

    @app.post("/work/<int:work_id>/bookmark")
    @login_required
    def toggle_bookmark(work_id: int):
        """作品のブックマーク追加/解除を行う。"""
        work = db.get_or_404(Work, work_id)
        action = (request.form.get("action") or "toggle").strip().lower()
        already_bookmarked = has_user_bookmarked(work.id, current_user.id)

        if action == "remove" or (action == "toggle" and already_bookmarked):
            if already_bookmarked:
                current_user.bookmarked_works.remove(work)
                db.session.commit()
                flash("マイリストから削除しました。", "info")
            else:
                flash("この作品はマイリストに登録されていません。", "warning")
        else:
            if already_bookmarked:
                flash("すでにマイリストに登録済みです。", "info")
            else:
                current_user.bookmarked_works.append(work)
                db.session.commit()
                flash("マイリストに追加しました。", "success")

        next_url = request.form.get("next", "")
        if is_safe_next_url(next_url):
            return redirect(next_url)

        return redirect(url_for("work_detail", work_id=work.id))

    @app.post("/work/<int:work_id>/tags")
    @login_required
    def add_tag_to_work(work_id: int):
        """ログインユーザーが作品へタグを追加する。"""
        work = db.get_or_404(Work, work_id)
        tag_name = normalize_tag_name(request.form.get("tag_name", ""))

        if not tag_name:
            flash("タグ名を入力してください。", "warning")
            return redirect(url_for("work_detail", work_id=work.id))

        try:
            # 既存タグを優先検索し、なければ新規作成する
            tag = Tag.query.filter_by(name=tag_name).first()
            if tag is None:
                tag = Tag(name=tag_name)
                db.session.add(tag)

            # 既に紐づいている場合は重複追加しない
            if tag in work.tags:
                flash("そのタグはすでに紐づいています。", "info")
            else:
                work.tags.append(tag)
                db.session.commit()
                flash("タグを紐づけました。", "success")
        except SQLAlchemyError as error:
            db.session.rollback()
            flash("タグの紐づけに失敗しました。", "warning")
            flash(f"エラー詳細: {format_safe_error_message(error)}", "info")

        return redirect(url_for("work_detail", work_id=work.id))

    @app.post("/api/work/<int:work_id>/add_tags")
    @login_required
    def api_add_tags_to_work(work_id: int):
        """インライン入力向け: 非同期でタグを追加し、JSONで結果を返す。"""
        work = db.get_or_404(Work, work_id)
        payload = request.get_json(silent=True) or {}
        raw_tags = payload.get("tags", "")
        parsed_tags = parse_tags_input(raw_tags)

        if not parsed_tags:
            return jsonify({"ok": False, "message": "追加するタグ名を入力してください。"}), 400

        added_tags: list[Tag] = []
        already_linked: list[str] = []

        try:
            for tag_name in parsed_tags:
                tag = Tag.query.filter_by(name=tag_name).first()
                if tag is None:
                    tag = Tag(name=tag_name)
                    db.session.add(tag)

                if tag in work.tags:
                    already_linked.append(tag_name)
                    continue

                work.tags.append(tag)
                added_tags.append(tag)

            db.session.commit()
        except SQLAlchemyError as error:
            db.session.rollback()
            return (
                jsonify(
                    {
                        "ok": False,
                        "message": "タグ追加に失敗しました。",
                        "error": format_safe_error_message(error),
                    }
                ),
                500,
            )

        message_parts = []
        if added_tags:
            message_parts.append(f"{len(added_tags)}件のタグを追加しました。")
        if already_linked:
            message_parts.append(f"{len(already_linked)}件は既に紐づいていました。")

        if not message_parts:
            message_parts.append("タグの変更はありませんでした。")

        return jsonify(
            {
                "ok": True,
                "work_id": work.id,
                "added_tags": [{"id": tag.id, "name": tag.name} for tag in added_tags],
                "already_linked": already_linked,
                "message": " ".join(message_parts),
            }
        )

    @app.post("/work/<int:work_id>/delete")
    @admin_required
    def delete_work(work_id: int):
        """作品自体を削除し、関連するタグ紐づけも解除する。"""
        work = db.get_or_404(Work, work_id)
        category = work.category
        work.bookmarked_by.clear()
        db.session.delete(work)
        db.session.commit()

        removed_count = cleanup_orphan_tags()

        flash("作品を削除しました。", "success")
        if removed_count > 0:
            flash(f"不要タグを{removed_count}件クリーンアップしました。", "info")

        return redirect(url_for("index", category=category))

    @app.post("/work/<int:work_id>/tags/<int:tag_id>/remove")
    @admin_required
    def remove_tag_from_work(work_id: int, tag_id: int):
        """作品とタグの紐づけだけを解除する（タグ自体は削除しない）。"""
        work = db.get_or_404(Work, work_id)
        tag = db.get_or_404(Tag, tag_id)

        if tag in work.tags:
            work.tags.remove(tag)
            db.session.commit()

            removed_count = cleanup_orphan_tags()

            flash("タグの紐づけを解除しました。", "success")
            if removed_count > 0:
                flash(f"不要タグを{removed_count}件クリーンアップしました。", "info")
        else:
            flash("そのタグはこの作品に紐づいていません。", "info")

        return redirect(url_for("work_detail", work_id=work.id))

    @app.get("/tag/<int:tag_id>")
    def tag_works(tag_id: int):
        """タグ別ページ: 指定タグに紐づく作品一覧を表示する。"""
        current_category = normalize_category(request.args.get("category"))
        tag = db.get_or_404(Tag, tag_id)
        works = (
            Work.query.join(Work.tags)
            .filter(Tag.id == tag_id, Work.category == current_category)
            .order_by(Work.id.desc())
            .all()
        )
        return render_template("tag_works.html", tag=tag, works=works, current_category=current_category)

    @app.get("/profile")
    @login_required
    def profile():
        """アカウント詳細とマイリストを表示する。"""
        bookmarked_works = (
            Work.query.join(bookmarks, Work.id == bookmarks.c.work_id)
            .filter(bookmarks.c.user_id == current_user.id)
            .order_by(bookmarks.c.created_at.desc(), Work.id.desc())
            .all()
        )
        return render_template("profile.html", bookmarked_works=bookmarked_works)

    return app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True)
