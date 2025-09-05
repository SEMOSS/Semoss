import {
    DatePicker as MuiDatePicker,
    DatePickerProps as MuiDatePickerProps,
} from '@mui/x-date-pickers';
import dayjs, { Dayjs } from 'dayjs';

export interface DatePickerProps
    extends Omit<
        MuiDatePickerProps,
        | 'value'
        | 'onChange'
        | 'minDate'
        | 'maxDate'
        | 'defaultValue'
        | 'referenceDate'
    > {
    value?: string | Date | Dayjs | null;
    onChange?: (value: string | null) => void;
    minDate?: string | Date | Dayjs;
    maxDate?: string | Date | Dayjs;
    defaultValue?: string | Date | Dayjs;
    referenceDate?: string | Date | Dayjs;
}

/**
 * Renders a date picker component that takes string values
 *
 * @component
 */
export const DatePicker = (props: DatePickerProps) => {
    const {
        value,
        onChange,
        minDate,
        maxDate,
        defaultValue,
        referenceDate,
        ...muiProps
    } = props;

    const getValidDayjs = (
        value: string | Date | Dayjs | null,
    ): Dayjs | null | undefined =>
        value ? dayjs(value) : (value as undefined | null);

    return (
        <MuiDatePicker
            {...muiProps}
            value={getValidDayjs(value)}
            onChange={(date) =>
                onChange?.(date ? date.format('YYYY-MM-DD') : null)
            }
            minDate={getValidDayjs(minDate)}
            maxDate={getValidDayjs(maxDate)}
            defaultValue={getValidDayjs(defaultValue)}
            referenceDate={getValidDayjs(referenceDate)}
        />
    );
};
