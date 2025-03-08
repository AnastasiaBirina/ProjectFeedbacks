import pytest
from airflow.models import DagBag

# Загружаем все DAG из папки dags
@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)

# Проверяем, что DAG загружен корректно
def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="first_dag")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 3  # Проверяем количество задач
    assert dag.schedule_interval == "0 9 * * *", 'TIME WRONG'

# Проверяем, что задачи имеют корректные зависимости
def test_task_dependencies(dagbag):
    dag = dagbag.get_dag(dag_id="first_dag")
    task1 = dag.get_task(task_id="task1")
    task2 = dag.get_task(task_id="task2")
    task3 = dag.get_task(task_id="task3")

    # Проверяем, что task1 >> task2 >> task3
    assert task1.downstream_task_ids == {"task2"}
    assert task2.downstream_task_ids == {"task3"}
    assert task3.downstream_task_ids == set() 
    
