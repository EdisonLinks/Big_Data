import os
from datetime import date, datetime

from flask import (
    Flask,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash

try:
    from .config import config
    from .repository import MySQLRepository
    from .services import RecommendationService
except ImportError:
    from config import config
    from repository import MySQLRepository
    from services import RecommendationService


def create_app(repository=None):
    app = Flask(__name__)
    app.config["SECRET_KEY"] = config.SECRET_KEY
    repo = repository or MySQLRepository()
    service = RecommendationService(repo)

    def current_user_id():
        return session.get("user_id")

    def require_login():
        if not current_user_id():
            flash("请先登录后查看个性化推荐。")
            return redirect(url_for("login"))
        return None

    @app.get("/")
    def index():
        if session.get("user_id"):
            return redirect(url_for("home"))
        return render_template("login.html")

    @app.get("/dashboard")
    def dashboard():
        return render_template("index.html", data=service.dashboard())

    @app.get("/visualization")
    def visualization():
        return render_template("visualization.html", data=service.visualization())

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "")
            member = repo.get_member_by_username(username)
            if member and check_password_hash(member["password_hash"], password):
                session["user_id"] = member["user_id"]
                session["username"] = member["username"]
                return redirect(url_for("home"))
            flash("用户名或密码不正确。")
        return render_template("login.html")

    @app.route("/register", methods=["GET", "POST"])
    def register():
        if request.method == "POST":
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "")
            if not username or not password:
                flash("用户名和密码不能为空。")
            elif repo.get_member_by_username(username):
                flash("用户名已存在，请直接登录。")
            else:
                member = repo.create_member(username, generate_password_hash(password))
                session["user_id"] = member["user_id"]
                session["username"] = member["username"]
                return redirect(url_for("home"))
        return render_template("register.html")

    @app.get("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))

    @app.get("/home")
    def home():
        redirect_response = require_login()
        if redirect_response:
            return redirect_response
        result = service.member_recommendations(current_user_id())
        return render_template("home.html", result=result)

    @app.get("/history")
    def history():
        redirect_response = require_login()
        if redirect_response:
            return redirect_response
        profile = service.member_profile(current_user_id())
        rows = repo.get_user_history(current_user_id(), 50)
        return render_template("history.html", profile=profile, history=rows)

    @app.get("/search")
    def search():
        redirect_response = require_login()
        if redirect_response:
            return redirect_response
        query = request.args.get("q", "").strip()
        rows = repo.search_meals(query, 40) if query and hasattr(repo, "search_meals") else []
        return render_template("search.html", query=query, results=rows)

    @app.post("/review")
    def submit_review():
        redirect_response = require_login()
        if redirect_response:
            return redirect_response

        user_id = current_user_id()
        meal_id = request.form.get("meal_id", "").strip()
        review = request.form.get("review", "").strip()
        try:
            rating = float(request.form.get("rating", ""))
        except ValueError:
            rating = 0

        if not meal_id:
            flash("缺少菜品 ID，无法提交点评。")
            return redirect(url_for("history"))
        if rating < 1 or rating > 5:
            flash("评分必须在 1 到 5 之间。")
            return redirect(url_for("history"))
        if not repo.user_has_ordered_meal(user_id, meal_id):
            flash("只能点评历史点过的菜品。")
            return redirect(url_for("history"))

        now = datetime.now()
        event = {
            "event_id": f"web-{user_id}-{meal_id}-{int(now.timestamp())}",
            "source": "flask_review_form",
            "user_id": user_id,
            "meal_id": meal_id,
            "rating": rating,
            "review": review,
            "review_date": date.today().isoformat(),
            "review_time": int(now.timestamp()),
            "event_time": int(now.timestamp()),
            "created_at": now.isoformat(timespec="seconds"),
        }
        try:
            repo.publish_review_event(event)
        except Exception:
            flash("消息队列暂不可用，评论已记录稍后重试。")
            return redirect(url_for("history"))
        flash("点评已提交到 Kafka，等待后台消费者写入数据库。")
        return redirect(url_for("meal_detail", meal_id=meal_id))

    @app.get("/meal/<meal_id>")
    def meal_detail(meal_id):
        meal = repo.get_meal(meal_id) or {"meal_id": meal_id, "meal_name": f"菜品 {meal_id}"}
        if hasattr(repo, "get_meal_review_stats"):
            stats = repo.get_meal_review_stats(meal_id)
            meal = dict(meal)
            meal["avg_rating"] = stats.get("avg_rating")
            meal["rating_count"] = stats.get("rating_count", 0)
        reviews = repo.get_meal_reviews(meal_id, 30)
        realtime_stats = (
            repo.get_realtime_meal_stats(meal_id)
            if hasattr(repo, "get_realtime_meal_stats")
            else None
        )
        similarities = service.meal_similarities(meal_id)
        return render_template(
            "meal_detail.html",
            meal=meal,
            reviews=reviews,
            realtime_stats=realtime_stats,
            similarities=similarities,
        )

    @app.get("/models")
    def models():
        return render_template("models.html", metrics=repo.get_model_metrics())

    @app.route("/recommend", methods=["GET", "POST"])
    def recommend():
        user_id = request.values.get("user_id", "A27H9DOUGY9FOS").strip()
        result = service.user_recommendations(user_id)
        return render_template("recommend.html", user_id=user_id, result=result)

    @app.route("/similar", methods=["GET", "POST"])
    def similar():
        meal_id = request.values.get("meal_id", "B00I3MPDP4").strip()
        result = service.meal_similarities(meal_id)
        return render_template("similar.html", meal_id=meal_id, result=result)

    @app.get("/status")
    def status():
        return render_template("status.html", counts=service.database_status())

    @app.get("/api/dashboard")
    def api_dashboard():
        return jsonify(service.dashboard())

    @app.get("/api/visualization")
    def api_visualization():
        return jsonify(service.visualization())

    @app.get("/api/recommend/<user_id>")
    def api_recommend(user_id):
        return jsonify(service.user_recommendations(user_id))

    @app.get("/api/similar/<meal_id>")
    def api_similar(meal_id):
        return jsonify(service.meal_similarities(meal_id))

    return app


app = create_app()


if __name__ == "__main__":
    host = os.getenv("FLASK_RUN_HOST", "127.0.0.1")
    port = int(os.getenv("FLASK_RUN_PORT", "5000"))
    app.run(host=host, port=port, debug=True)
