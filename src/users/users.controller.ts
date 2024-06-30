import { Controller, Post } from '@nestjs/common';
import { ProcessUsersJob } from './process-users.job';
import { UsersService } from './users.service';

@Controller('users')
export class UsersController {
  constructor(
    private readonly usersService: UsersService,
    private readonly processUsersJob: ProcessUsersJob,
  ) {}

  @Post('seed')
  seed() {
    return this.usersService.seedDatabase();
  }

  @Post('process')
  process() {
    return this.processUsersJob.run();
  }
}
