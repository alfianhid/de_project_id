import requests
from airflow.models import Variable


def send_slack_alert(context: dict) -> None:
    """
    Airflow task failure callback.
    Usage: add to DAG default_args as on_failure_callback=send_slack_alert
    """
    webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
    if not webhook_url:
        return

    ti      = context["task_instance"]
    payload = {
        "text": (
            f"🚨 *Airflow Task Failed*\n"
            f"*DAG:*  `{context['dag'].dag_id}`\n"
            f"*Task:* `{ti.task_id}`\n"
            f"*Run:*  `{context['execution_date']}`\n"
            f"*Log:*  <{ti.log_url}|View logs>"
        )
    }
    requests.post(webhook_url, json=payload, timeout=10)