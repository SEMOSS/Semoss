import { LogOut, User } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
	DropdownMenu,
	DropdownMenuContent,
	DropdownMenuItem,
	DropdownMenuLabel,
	DropdownMenuSeparator,
	DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { useAppContext } from "@/contexts";

/**
 * Renders a menu showing users their name and allowing them to log out
 *
 * @component
 */
export const UserProfileMenu = () => {
	const { logout, userLoginName } = useAppContext();

	/**
	 * Functions
	 */
	const handleLogout = async () => {
		const success = await logout();
		if (success) localStorage.clear();
		window.location.reload();
	};

	return (
		<DropdownMenu>
			<DropdownMenuTrigger asChild>
				<Button variant="ghost" size="icon" title="View user menu">
					<User className="h-5 w-5" />
				</Button>
			</DropdownMenuTrigger>
			<DropdownMenuContent align="end" className="w-56">
				<DropdownMenuLabel className="flex items-center space-x-2">
					<User className="h-4 w-4" />
					<span>{userLoginName}</span>
				</DropdownMenuLabel>
				<DropdownMenuSeparator />
				<DropdownMenuItem onClick={handleLogout}>
					<LogOut className="h-4 w-4 mr-2" />
					Logout
				</DropdownMenuItem>
			</DropdownMenuContent>
		</DropdownMenu>
	);
};
