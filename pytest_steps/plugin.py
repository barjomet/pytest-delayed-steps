# Authors: Sylvain MARIE <sylvain.marie@se.com>
#          + All contributors to <https://github.com/smarie/python-pytest-steps>
#
# License: 3-clause BSD, <https://github.com/smarie/python-pytest-steps/blob/master/LICENSE>
import pytest
from collections import defaultdict, deque
from pytest import Session
from pytest_steps.steps import cross_steps_fixture
from pytest_steps.steps_generator import one_fixture_per_step, GENERATOR_MODE_STEP_ARGNAME

try:
    from pytest_steps import pivot_steps_on_df, handle_steps_in_results_df
except ImportError:
    # this is normal if pytest-harvest is not installed
    pass
else:
    @pytest.fixture(scope='function')
    def session_results_df_steps_pivoted(request, session_results_df):
        """
        A pivoted version of fixture `session_results_df` from pytest_harvest.
        In this version, there is one row per test with the results from all steps in columns.
        """
        # Handle the steps
        session_results_df = handle_steps_in_results_df(session_results_df, keep_orig_id=False)

        # Pivot
        return pivot_steps_on_df(session_results_df, pytest_session=request.session)

    @pytest.fixture(scope='function')
    def module_results_df_steps_pivoted(request, module_results_df):
        """
        A pivoted version of fixture `module_results_df` from pytest_harvest.
        In this version, there is one row per test with the results from all steps in columns.
        """
        # Handle the steps
        module_results_df = handle_steps_in_results_df(module_results_df, keep_orig_id=False)

        # Pivot
        return pivot_steps_on_df(module_results_df, pytest_session=request.session)

    @pytest.fixture
    @one_fixture_per_step
    def step_bag(results_bag):
        """
        Provides a separate pytest-harvest "results_bag" per step
        """
        return results_bag

    @pytest.fixture
    @cross_steps_fixture
    def cross_bag(results_bag):
        """
        Provides a cross-step pytest-harvest "results_bag" for explicit mode
        """
        return results_bag


def pytest_collection_finish(session: Session):
    items_reordered = []
    delayed_steps = defaultdict(deque)

    def process_delayed_steps():
        while delayed_steps:
            for origname in list(delayed_steps):
                if not delayed_steps[origname]:
                    del delayed_steps[origname]
                    continue
                items_reordered.append(delayed_steps[origname].popleft())

    for item in session.items:
        if item.get_closest_marker('steps_delayed'):
            steps_id_index = list(item.callspec.params).index(GENERATOR_MODE_STEP_ARGNAME)
            steps_group_key = (
                item.originalname,
                *(i for n, i in enumerate(item.callspec._idlist) if n != steps_id_index)
            )
            delayed_steps[steps_group_key].append(item)
        else:
            process_delayed_steps()
            items_reordered.append(item)
    process_delayed_steps()
    session.items = items_reordered
