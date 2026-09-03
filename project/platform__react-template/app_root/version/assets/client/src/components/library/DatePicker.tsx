import { format } from "date-fns";
import { Calendar as CalendarIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Calendar } from "@/components/ui/calendar";
import {
	Popover,
	PopoverContent,
	PopoverTrigger,
} from "@/components/ui/popover";

export interface DatePickerProps {
	value: string | null; // YYYY-MM-DD
	onChange: (value: string | null) => void;
	label?: string;
	placeholder?: string;
	maxDate?: Date;
	minDate?: Date;
	disabled?: boolean;
}

/**
 * Date picker component using shadcn calendar
 *
 * @component
 */
export const DatePicker = ({
	value,
	onChange,
	placeholder = "Pick a date",
	maxDate,
	minDate,
	disabled = false,
}: DatePickerProps) => {
	const dateValue = value
		? (() => {
				const [year, month, day] = value.split("-").map(Number);
				return new Date(year, month - 1, day);
			})()
		: undefined;

	const handleSelect = (date: Date | undefined) => {
		if (date) {
			// Format as YYYY-MM-DD for consistency
			const formattedDate = format(date, "yyyy-MM-dd");
			onChange(formattedDate);
		} else {
			onChange(null);
		}
	};

	return (
		<Popover>
			<PopoverTrigger asChild>
				<Button
					variant="outline"
					className={`w-full justify-start text-left font-normal ${dateValue ? "text-muted-foreground" : ""}`}
					disabled={disabled}
				>
					<CalendarIcon className="mr-2 h-4 w-4" />
					{dateValue ? (
						format(dateValue, "PPP")
					) : (
						<span>{placeholder}</span>
					)}
				</Button>
			</PopoverTrigger>
			<PopoverContent className="w-auto p-0" align="start">
				<Calendar
					mode="single"
					selected={dateValue}
					onSelect={handleSelect}
					disabled={(date) => {
						if (maxDate && date > maxDate) return true;
						if (minDate && date < minDate) return true;
						return false;
					}}
				/>
			</PopoverContent>
		</Popover>
	);
};
