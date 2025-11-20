package fr.aneo.armonik.worker.samples;

import fr.aneo.armonik.worker.domain.TaskContext;
import fr.aneo.armonik.worker.domain.TaskOutcome;
import fr.aneo.armonik.worker.domain.TaskProcessor;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SumProcessor implements TaskProcessor {

  @Override
  public TaskOutcome processTask(TaskContext taskContext) {
    var num1 = Integer.parseInt(taskContext.getInput("num1").asString(UTF_8));
    var num2 = Integer.parseInt(taskContext.getInput("num2").asString(UTF_8));

    taskContext.getOutput("result").write(String.valueOf(num1 + num2).getBytes(UTF_8));

    return TaskOutcome.SUCCESS;
  }
}
