import { Queue } from 'bull';
import { DataSource } from 'typeorm';
import { InjectQueue, Process, Processor } from '@nestjs/bull';
import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { User } from './user.entity';

const QUEUE_NAME = 'cron-jobs';
const JOB_NAME = 'process-users';

const BATCH_SIZE = 10000;
const BATCH_DELAY = 1000;
const SLICE_SIZE = 1000;

type UserRecord = {
  id: number;
  status: 'pending' | 'processed';
};

@Injectable()
@Processor(QUEUE_NAME)
export class ProcessUsersJob {
  private readonly logger = new Logger(ProcessUsersJob.name);

  constructor(
    private readonly dataSource: DataSource,
    @InjectQueue(QUEUE_NAME)
    private readonly queue: Queue,
  ) {}

  @Cron(CronExpression.EVERY_DAY_AT_3AM)
  async run() {
    // adicionar numa fila
    await this.queue.add(JOB_NAME);
    this.logger.log(`Initial job added to the queue`);
  }

  @Process(JOB_NAME)
  protected async process() {
    this.logger.log(`Processing started`);

    // buscar x registros
    const batch = await this.queryBatch();

    // processar
    await this.processBatch(batch);

    // agendar a próxima execução
    if (batch.length === BATCH_SIZE) {
      await this.queue.add(JOB_NAME, { delay: BATCH_DELAY });
      this.logger.log(
        `More entries to process, scheduling next job with ${BATCH_DELAY}ms delay`,
      );
    }
  }

  private async queryBatch() {
    this.logger.log(`Querying batch`);
    return this.dataSource.query<UserRecord[]>(
      `select id, status from users where status = 'pending' limit ${BATCH_SIZE} for update skip locked`,
    );
  }

  private async processBatch(batch: UserRecord[]) {
    this.logger.log(`Processing batch of ${batch.length} records`);
    for (let i = 0; i < batch.length; i += SLICE_SIZE) {
      const slice = batch.slice(i, i + SLICE_SIZE);
      const results = await Promise.allSettled(
        slice.map((record) => this.processRecord(record)),
      );
      results.forEach((result, index) => {
        if (result.status === 'rejected') {
          this.logger.error(
            `Error processing record ${slice[index].id}: ${result.reason}`,
          );
        }
      });
    }
    this.logger.log(`Batch processed`);
  }

  private async processRecord(record: UserRecord) {
    this.logger.log(`Processing record ${record.id}`);
    await this.dataSource.manager.update(User, record.id, {
      status: 'processed',
    });
    await new Promise((resolve) =>
      setTimeout(resolve, Math.random() * (500 - 200) + 200),
    );
    if (Math.random() < 0.1) {
      throw new Error('Random error');
    }
  }
}
