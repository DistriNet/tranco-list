from redis import Redis
from rq import Queue
import combined_lists
import notify_email


class JobHandler:
    def __init__(self):
        self.setup_job_queues()

    def setup_job_queues(self):
        self.conn = Redis('localhost', 6379)
        self.generate_queue = Queue('generate', connection=self.conn)
        self.email_queue = Queue('notify_email', connection=self.conn)
        return self.generate_queue

    def submit_generate_job(self, config, list_id):
        if list_id not in self.current_jobs():
            self.generate_queue.enqueue(combined_lists.generate_combined_list, config, list_id, job_id=str(list_id), timeout=600)

    def submit_email_job(self, email_address, list_id, list_size):
        generate_job = self.generate_queue.fetch_job(list_id)
        self.email_queue.enqueue(notify_email.send_notification, email_address, list_id, list_size, depends_on=generate_job)

    def current_jobs(self):
        return self.generate_queue.job_ids

    def jobs_ahead_of_job(self, list_id):
        if list_id in self.current_jobs():
            return self.current_jobs().index(list_id)
        else:
            return None

    def get_job_status(self, list_id):
        return {"completed": self.get_job_success(list_id) is not None, "jobs_ahead": self.jobs_ahead_of_job(list_id), "success": self.get_job_success(list_id)}

    def get_job_success(self, list_id):
        return self.generate_queue.fetch_job(list_id).result

    def delete_if_failure(self):
        return "TODO"
