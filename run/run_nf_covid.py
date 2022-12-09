from utils.utils import roots
from typing import Dict, List

from jobmon.client.tool import Tool

from run.constants import (
    LongSaveConstants,
    ShortSaveConstants,
    JobmonConstants,
    PipelineConstants,
)


class RunNfCovid(object):
    """Instances of this class represent a long covid pipeline run.
    """
    def __init__(
        self,
        output_version: str,
        job_name: str,
        locations: List[int],
        logs_loc: str,
        age_groups: List[int],
        location_set_id: int,
        input_data_version: str,
        hsp_icu_input_path: str,
        infect_death_input_path: str,
        definition: str,
        release_id: int,
        estimation_years: List[int],
        all_gbd_estimation_years: List[int],
        db_description: str,
        mark_as_best: bool = False,
        save_incidence: bool = False,
    ) -> None:
        """Creates a new ``Nonfatal long covid Jobmon`` model instance.

        Args:
            output_version (str): Name of the output version the data is being
                written to
            job_name (str): Name of the output folder the data is being written
                to
            locations (List[int]): List of locations to run the pipeline on
            logs_loc (str): The path to the error and output jobmon logs 
            age_groups (List[int]): List of age groups to run the pipeline on
            location_set_id (int): The location set to run the pipeline on
            input_data_version (str): The hospitalizations and infections input
                data version
            hsp_icu_input_path (str): The path to the icu hospitalization 
                pipeline estimates
            infect_death_input_path (str): The path to the infected deaths 
                pipeline estimates
            definition (str): How the acute phase is defined
            release_id (int): The release id to use for shared functions
            estimation_years (List[int]): List of estimation years (starts
                after 2019)
            all_gbd_estimation_years (List[int]): List of estimation years that
                includes both gbd estimation years and pipeline years
            db_description (str): Description to add to the database if saving
                run to Epi db
            mark_as_best (bool): If ``True``, then marks run as best if saving
                to Epi db. Defaults to ``False``.
            save_incidence (bool): If ``True``, then saves measure 6 for all
                MEs. Defaults to ``False``.
        """
        self.output_version = output_version
        self.location_ids = locations
        self.logs_loc = logs_loc
        self.age_groups = age_groups
        self.location_set_id = location_set_id
        self.job_name = job_name
        self.input_data_version = input_data_version
        self.hsp_icu_input_path = hsp_icu_input_path
        self.infect_death_input_path = infect_death_input_path
        self.definition = definition
        self.release_id = release_id
        self.estimation_years = estimation_years
        self.all_gbd_estimation_years = all_gbd_estimation_years
        self.db_description = db_description
        self.mark_as_best = mark_as_best
        self.save_incidence = save_incidence

        self.pipeline = Tool("long_covid_pipeline")

        self.diagnostic_tasks = []
        self.short_cov_list = []
        self.long_cov_list = []

    def create_workflow(self) -> None:
        """Creates Jobmon workflow with passed in names."""
        self.wf = self.pipeline.create_workflow(
            name=f'{self.job_name}',
            default_cluster_name='slurm',
            workflow_args=self.job_name
        )

    def create_task_template(self) -> None:
        """Formats Jobmon task templates for all 5 parts of the pipeline."""
        self.short_covid_template = self.pipeline.get_task_template(
            template_name="short_covid_task",
            command_template=(
                "{r_executable} {script} {location_id} "
                "{output_version} {hsp_icu_input_path} {estimation_years} "
                "{age_groups} {location_set_id} {release_id}"
            ),
            node_args=["location_id"],
            task_args=[
                "output_version",
                "hsp_icu_input_path",
                "estimation_years",
                "age_groups",
                "location_set_id",
                "release_id",
            ],
            op_args=["r_executable", "script"])
        self.long_covid_template = self.pipeline.get_task_template(
            template_name="long_covid_task",
            command_template=(
                "{r_executable} {script} {location_id} "
                "{output_version} {definition} {hsp_icu_input_path} "
                "{estimation_years} {age_groups} {location_set_id} "
                "{release_id}"
            ),
            node_args=["location_id"],
            task_args=[
                "output_version",
                "definition",
                "hsp_icu_input_path",
                "estimation_years",
                "age_groups",
                "location_set_id",
                "release_id",
            ],
            op_args=["r_executable", "script"]
        )
        self.s_save_results_template = self.pipeline.get_task_template(
            template_name="short_save_results_task",
            command_template=(
                "{r_executable} {script} {output_version} {measure} "
                "{location_set_id} {release_id} {mark_as_best} "
                "{save_incidence} {all_gbd_estimation_years} {db_description}"
            ),
            node_args=["measure"],
            task_args=[
                "output_version",
                "location_set_id",
                "release_id",
                "mark_as_best",
                "save_incidence",
                "all_gbd_estimation_years",
                "db_description"
            ],
            op_args=["r_executable", "script"])
        self.l_save_results_template = self.pipeline.get_task_template(
            template_name="long_save_results_task",
            command_template=(
                "{r_executable} {script} {output_version} "
                "{measure} {location_set_id} {release_id} {mark_as_best} "
                "{save_incidence} {all_gbd_estimation_years} {db_description}"
            ),
            node_args=["measure"],
            task_args=[
                "output_version",
                "location_set_id",
                "release_id",
                "mark_as_best",
                "save_incidence",
                "all_gbd_estimation_years",
                "db_description"
            ],
            op_args=["r_executable", "script"])
        self.diagnostics_template = self.pipeline.get_task_template(
            template_name="diagnostics_task",
            command_template=(
                "{r_executable} {script} {output_version} "
                "{loc_id} {all_diagnostics}"
            ),
            task_args=["output_version"],
            node_args=["loc_id", "all_diagnostics"],
            op_args=["r_executable", "script"])

    def create_short_long_tasks(self) -> None:
        """Formats Jobmon tasks for the short and long covid parts of the
        pipeline for each location.
        """
        for loc in self.location_ids:
            s_task = self.short_covid_template.create_task(
                compute_resources=self._get_compute_resources(
                    cores=10,
                    memory=160,
                    runtime=86400
                ),
                name=f'short_{loc}',
                max_attempts=JobmonConstants.MAX_ATTEMPTS,
                r_executable=JobmonConstants.R_EXE,
                script=f"{roots['nf_repo']}src/4_short_covid_multi.R",
                location_id=loc,
                output_version=self.output_version,
                hsp_icu_input_path=self.hsp_icu_input_path,
                estimation_years=self.estimation_years,
                age_groups=self.age_groups,
                location_set_id=self.location_set_id,
                release_id=self.release_id)

            l_task = self.long_covid_template.create_task(
                compute_resources=self._get_compute_resources(
                    cores=20,
                    memory=400,
                    runtime=86400
                ),
                name=f'long_cov_{loc}',
                max_attempts=JobmonConstants.MAX_ATTEMPTS,
                upstream_tasks=[s_task],
                r_executable=JobmonConstants.R_EXE,
                script=f"{roots['nf_repo']}src/6_long_covid.R",
                location_id=loc,
                output_version=self.output_version,
                definition=self.definition,
                hsp_icu_input_path=self.hsp_icu_input_path,
                estimation_years=self.estimation_years,
                age_groups=self.age_groups,
                location_set_id=self.location_set_id,
                release_id=self.release_id)

            self.short_cov_list.append(s_task)
            self.long_cov_list.append(l_task)
            self.wf.add_task(s_task)
            self.wf.add_task(l_task)

    def create_short_save_results_tasks(self) -> None:
        """Formats Jobmon tasks for the short covid save results part of the
        pipeline for each short covid measure.
        """
        for measure in ShortSaveConstants.MEASURE_SHORT:
            s_save_results_task = self.s_save_results_template.create_task(
                compute_resources=self._get_compute_resources(
                    cores=10,
                    memory=55,
                    runtime=36000
                ),
                name=f'short_save_results_{measure}',
                upstream_tasks=self.short_cov_list,
                max_attempts=JobmonConstants.MAX_ATTEMPTS,
                r_executable=JobmonConstants.R_EXE,
                script=f"{roots['nf_repo']}src/5_short_save_results.R",
                output_version=self.output_version,
                measure=measure,
                location_set_id=self.location_set_id,
                release_id=self.release_id,
                mark_as_best=self.mark_as_best,
                save_incidence=self.save_incidence,
                all_gbd_estimation_years=self.all_gbd_estimation_years,
                db_description=self.db_description)

            self.wf.add_task(s_save_results_task)

    def create_long_save_results_tasks(self) -> None:
        """Formats Jobmon tasks for the long covid save results part of the
        pipeline for each long covid measure.
        """
        for measure in LongSaveConstants.MEASURE_LONG:
            l_save_results_task = self.l_save_results_template.create_task(
                compute_resources=self._get_compute_resources(
                    cores=10,
                    memory=55,
                    runtime=36000
                ),
                name=f'long_save_results_{measure}',
                upstream_tasks=self.long_cov_list,
                max_attempts=JobmonConstants.MAX_ATTEMPTS,
                r_executable=JobmonConstants.R_EXE,
                script=f"{roots['nf_repo']}src/7_long_save_results.R",
                output_version=self.output_version,
                measure=measure,
                location_set_id=self.location_set_id,
                release_id=self.release_id,
                mark_as_best=self.mark_as_best,
                save_incidence=self.save_incidence,
                all_gbd_estimation_years=self.all_gbd_estimation_years,
                db_description=self.db_description)

            self.wf.add_task(l_save_results_task)

    def create_diagnostics_tasks(self) -> None:
        """Formats Jobmon tasks for the diagnostics part of the pipeline for
        each location.
        """
        last_loc = self.location_ids[-1]

        for loc in self.location_ids:
            if loc != last_loc:
                diag_task = self.diagnostics_template.create_task(
                    compute_resources=self._get_compute_resources(
                        cores=2,
                        memory=30,
                        runtime=1800
                    ),
                    name=f'diagnostics_{loc}',
                    upstream_tasks=self.long_cov_list + self.short_cov_list,
                    max_attempts=JobmonConstants.MAX_ATTEMPTS,
                    r_executable=JobmonConstants.R_EXE,
                    script=f"{roots['nf_repo']}src/8_diagnostics.R",
                    output_version=self.output_version,
                    loc_id=loc,
                    all_diagnostics=False)

                self.diagnostic_tasks.append(diag_task)
            else:
                diag_task = self.diagnostics_template.create_task(
                    compute_resources=self._get_compute_resources(
                        cores=20,
                        memory=300,
                        runtime=54000
                    ),
                    name=f'diagnostics_{loc}',
                    upstream_tasks=self.long_cov_list
                    + self.short_cov_list + self.diagnostic_tasks,
                    max_attempts=JobmonConstants.MAX_ATTEMPTS,
                    r_executable=JobmonConstants.R_EXE,
                    script=f"{roots['nf_repo']}src/8_diagnostics.R",
                    output_version=self.output_version,
                    loc_id=loc,
                    all_diagnostics=True)

            self.wf.add_task(diag_task)

    def run(self) -> str:
        """Kicks off the workflow with assigned timeout and gives the option
        to resume if workflow fails.
        """
        return self.wf.run(
            seconds_until_timeout=260000,
            resume=True
        )

    def _get_compute_resources(
        self,
        cores: int,
        memory: int,
        runtime: int
    ) -> Dict:
        """Assigns all cluster job specs."""
        return {
            'stderr': f"{self.logs_loc}errors",
            'stdout': f"{self.logs_loc}output",
            'project': "proj_nfrqe",
            'cores': cores,
            'memory': memory,
            'queue': PipelineConstants.DEAFULT_QUEUE,
            'runtime': runtime,
            'working_directory': roots['nf_repo'],
            'seconds_until_timeout': 43200,
        }
