"""Tests for ETL utils actions
"""

import os
import unittest 
import mock

from testfixtures import TempDirectory
from nose.tools import raises
from nose.tools import eq_

from ..utils import process_steps

class UtilsTests(unittest.TestCase):


    def setUp(self):
      self.steps_params = [{'database': 'staging',
	  'host_name': 'sandbox',
	  'name': 'extract-things-staging_table',
	  'sql': "select id from things;  \n",
          'step_type': "extract-rds"},
	  {'depends_on': 'extract-things-staging_table',
	      'input_node': 'extract-things-staging_table',
	      'insert_mode': 'TRUNCATE',
	      'schema': 'things_data',
	      'table': 'things_staging',
              'step_type': 'load-redshift'},
	  {'script': 'sql/thing_facts.sql',
           'step_type': "sql-command"}]

          
    def test_process_steps_step_type(self):
      test_steps = process_steps(self.steps_params)

      # does each entry have a "step_class"
      result = [x for x in test_steps if 'step_class' in x]
      #result = list(filter(lambda x: 'step_class' in x, test_steps))
      eq_(len(result), 3)

    def test_process_steps_translates_relative_path(self):
      mock_patch = mock.patch('os.path.isfile')  
      mock_thing = mock_patch.start()
      mock_thing.side_effect = lambda x: True
      test_path = "/path/to/definition.yaml"
      test_steps = process_steps(self.steps_params, test_path)

      result = test_steps[2]['script']
      assert result.startswith("/path/to")
      assert result.endswith("sql/thing_facts.sql")

