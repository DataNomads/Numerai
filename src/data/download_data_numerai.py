import os
import numerapi
import luigi
from dotenv import find_dotenv, load_dotenv


class DownloadAndExtractData(luigi.Task):
    """
    Download the most recent data and extract data to ./data/raw by default.

    :param: output_path
    :param: public_id (from .env)
    :param: secret_key (from .env)

    Download data using NumerAPI and extract following files::

    Extends:
        luigi.task
    """
    task_namespace = 'download_data'
    output_path = luigi.Parameter(default="./data/raw")
    load_dotenv(find_dotenv())
    public_id = luigi.Parameter(os.getenv("public_id"))
    secret_key = luigi.Parameter(os.getenv("secret_key"))

    def output(self):
        try:
            self.api = numerapi.NumerAPI(self.public_id, self.secret_key)
        except ValueError:
            print("Incorrect public_id or secret_key")

        current_round = self.api.get_current_round()
        dataset_name = "numerai_dataset_{0}.zip".format(current_round)
        dataset_dir = "numerai_dataset_{0}".format(current_round)

        # if false, assert throws an AssertionError
        # download current dataset
        assert self.api.download_current_dataset(dest_path=self.output_path,
                                                 dest_filename=dataset_name,
                                                 unzip=True,
                                                 tournament=1)

        dataset_path = os.path.join(self.output_path, dataset_dir)

        test_data_path = os.path.join(dataset_path, 'numerai_training_data.csv')
        tournament_data_path = os.path.join(dataset_path,
                                            'numerai_tournament_data.csv')
        example_data_path = os.path.join(dataset_path,
                                         'example_predictions.csv')

        out = {
            'zipfile': luigi.LocalTarget(os.path.join(self.output_path, dataset_name)),
            'training_data.csv': luigi.LocalTarget(test_data_path),
            'tournament_data.csv': luigi.LocalTarget(tournament_data_path),
            'example_predictions.csv': luigi.LocalTarget(example_data_path)
        }
#         print(out)
        return out

    def run(self):
        out = self.output


if __name__ == '__main__':
    # load_dotenv(find_dotenv())
    # public_id = luigi.Parameter(os.getenv("public_id"))
    # secret_key = luigi.Parameter(os.getenv("secret_key"))
    luigi.run()
