from flask import Blueprint, jsonify, request

from job_manager import PriceBatchJob, DualApiSequentialJob


def create_job_blueprint(client, get_session) -> Blueprint:
    job = PriceBatchJob(client=client, get_session=get_session)
    bp = Blueprint("job", __name__)

    @bp.route("/api/admin/job/status", methods=["GET"])
    def job_status():
        return jsonify(job.status())

    @bp.route("/api/admin/job/start", methods=["POST"])
    def job_start():
        payload = request.get_json(silent=True) or {}
        start_id = payload.get("startId")
        batch_size = payload.get("batchSize")
        interval_sec = payload.get("intervalSec")
        data = job.start(start_id, batch_size, interval_sec)
        return jsonify(data)

    @bp.route("/api/admin/job/pause", methods=["POST"])
    def job_pause():
        return jsonify(job.pause())

    @bp.route("/api/admin/job/resume", methods=["POST"])
    def job_resume():
        return jsonify(job.resume())

    @bp.route("/api/admin/job/stop", methods=["POST"])
    def job_stop():
        return jsonify(job.stop())

    return bp


def create_dual_job_blueprint(client1, client2, get_session) -> Blueprint:
    job = DualApiSequentialJob(client1=client1, client2=client2, get_session=get_session)
    bp = Blueprint("dualjob", __name__)

    @bp.route("/api/admin/dualjob/status", methods=["GET"])
    def dual_job_status():
        return jsonify(job.status())

    @bp.route("/api/admin/dualjob/start", methods=["POST"])
    def dual_job_start():
        payload = request.get_json(silent=True) or {}
        start_id = payload.get("startId")
        batch_size = payload.get("batchSize")
        interval_sec = payload.get("intervalSec")
        data = job.start(start_id, batch_size, interval_sec)
        return jsonify(data)

    @bp.route("/api/admin/dualjob/pause", methods=["POST"])
    def dual_job_pause():
        return jsonify(job.pause())

    @bp.route("/api/admin/dualjob/resume", methods=["POST"])
    def dual_job_resume():
        return jsonify(job.resume())

    @bp.route("/api/admin/dualjob/stop", methods=["POST"])
    def dual_job_stop():
        return jsonify(job.stop())

    return bp