import { CommandOptionType, ScheduleEntry, utils, Value } from '@yamcs/webapp-sdk';
import { TemplateProvider } from '../../command-sender/command-form/TemplateProvider';

/**
 * Pre-fills the command form with the arguments of an existing schedule entry.
 */
export class ScheduleEntryTemplateProvider implements TemplateProvider {
  constructor(private entry: ScheduleEntry) {}

  getAssignment(argumentName: string): Value | undefined {
    const args = this.entry.args || {};
    if (argumentName in args) {
      return utils.toValue(args[argumentName]);
    }
    return undefined;
  }

  getOption(id: string, expectedType: CommandOptionType): Value | undefined {
    return undefined;
  }

  getComment() {
    return undefined;
  }

  getStream() {
    return undefined;
  }

  isDisableTransmissionConstraints() {
    return false;
  }

  getAdvancementParams() {
    return undefined;
  }
}
