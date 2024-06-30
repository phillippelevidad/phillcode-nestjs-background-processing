import { Repository } from 'typeorm';
import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { User } from './user.entity';

@Injectable()
export class UsersService {
  constructor(
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,
  ) {}

  async seedDatabase(totalSize = 500_000, batchSize = 1000) {
    let count = 0;
    for (const batch of this.generateUsers(totalSize, batchSize)) {
      await this.userRepository.save(batch);
      count += batch.length;
      console.log(`Inserted ${count} users`);
    }
    console.log('Database seeding completed');
  }

  private *generateUsers(
    totalSize: number,
    batchSize: number,
  ): Generator<any[], void, unknown> {
    let count = 0;
    while (count < totalSize) {
      const batch = [];
      for (let i = 0; i < batchSize && count < totalSize; i++, count++) {
        const user = this.userRepository.create({ status: 'pending' });
        batch.push(user);
      }
      yield batch;
    }
  }
}
