import json

__author__ = 'samuels'


class JSONParser:
    def __init__(self, logger, path_to_file):
        self.logger = logger
        self.path_to_file = path_to_file
        self._scenario_data = None

        try:
            with open(self.path_to_file) as f:
                self._scenario_data = json.load(f)
        except IOError:
            self.logger.exception("Failed open %s" % self.path_to_file)
            raise IOError
        self._objects_count = len(self.scenario_data['FluidFS_file'])
        if not self._objects_count:
            self.logger.exception("Bad file %s. Can't find file objects definitions" % self.path_to_file)
            raise RuntimeError()

    @property
    def scenario_data(self):
        return self._scenario_data

    @property
    def objects_count(self):
        return self._objects_count
