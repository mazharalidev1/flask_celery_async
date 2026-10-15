from examples.monitoring.main import progress_task


if __name__ == "__main__":
    result = progress_task.delay(40)

    print(f"Task submitted: {result.id}")