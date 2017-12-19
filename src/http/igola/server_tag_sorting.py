# coding: utf-8

from conf.env import TASK_REPO_HOME
from http.server_tag import TagServer
from http.register import http_register, http_route, http_method


@http_register
class IGola_TagSortingServer(TagServer):

    _repository = TASK_REPO_HOME + "/igola/sort"
    _tag_task_module = ["task.igola.tag_sort", "IGola_TaskTagSorting"]
    _task_record = {}

    @http_method("GET")
    @http_route("/pio/tag-top10")
    def nothing(self):
        return "nothing"

    @http_method("POST")
    @http_route("/pio/tag-top10")
    def create(self, *args, **kwargs):
        task_name = '_'.join(kwargs["id"])
        argv = self.get_task_params(
            task_name,
            repo_data_file=self.get_repository_data_file(task_name),
            group_id=[kwargs["usergroup_id"]]
        )
        return self.task_run(task_name, argv)

    @http_method("GET")
    @http_route("/pio/tag-top10")
    def status(self, *args, **kwargs):
        task_name = '_'.join(kwargs["id"])
        return self.task_status(task_name)

    @http_method("GET")
    @http_route("/pio/tag-top10")
    def query(self, *args, **kwargs):
        task_name = '_'.join(kwargs["id"])
        return self.task_query(task_name)

    @http_method("DELETE")
    @http_route("/pio/tag-top10")
    def delete(self, *args, **kwargs):
        task_name = '_'.join(kwargs["id"])
        return self.task_delete(task_name)

