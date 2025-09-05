import { useAppContext } from '@/contexts';
import { useLoadingState } from '@/hooks';
import { AccountCircle, Logout } from '@mui/icons-material';
import { Button, Menu, Stack, Typography } from '@mui/material';

export interface UserProfileMenuProps {
    open: boolean;
    anchorEl: Element;
}

/**
 * Renders a menu showing users their name and allowing them to log out
 *
 * @component
 */
export const UserProfileMenu = ({ open, anchorEl }: UserProfileMenuProps) => {
    const { logout, userLoginName } = useAppContext();

    /**
     * State
     */
    const [isLogoutLoading, setIsLogoutLoading] = useLoadingState();

    /**
     * Functions
     */
    const handleLogout = async () => {
        const loadingKey = setIsLogoutLoading(true);
        const success = await logout();
        if (success) localStorage.clear();
        window.location.reload();
        setIsLogoutLoading(false, loadingKey);
    };

    return (
        <Menu open={open} anchorEl={anchorEl}>
            <Stack spacing={1} alignItems="center" padding={1}>
                <Stack direction="row" spacing={1}>
                    <AccountCircle color="action" />

                    <Typography variant="body1">{userLoginName}</Typography>
                </Stack>

                <Button
                    title="Logout"
                    endIcon={<Logout />}
                    onClick={handleLogout}
                    variant="contained"
                    size="small"
                    loading={isLogoutLoading}
                >
                    Logout
                </Button>
            </Stack>
        </Menu>
    );
};
