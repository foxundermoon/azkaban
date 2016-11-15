package azkaban.user;

import java.util.HashMap;
import java.util.Set;

/**
 * Created by bianzexin on 16/11/15.
 */
public class UserUtils {

    public static UserUtils instance = new UserUtils();

    public static UserUtils getInstance() {
        return instance;
    }

    public void resolveGroupRoles(HashMap<String, Set<String>> groupRoles, User user) {
        for (String group : user.getGroups()) {
            Set<String> groupRoleSet = groupRoles.get(group);
            if (groupRoleSet != null) {
                for (String role : groupRoleSet) {
                    user.addRole(role);
                }
            }
        }
    }

    public User getUser(HashMap<String, Set<String>> groupRoles, HashMap<String, User> users, HashMap<String, String> userPassword, String
            username, String password) throws
            UserManagerException {
        if (username == null || username.trim().isEmpty()) {
            throw new UserManagerException("Username is empty.");
        } else if (password == null || password.trim().isEmpty()) {
            throw new UserManagerException("Password is empty.");
        }

        // Minimize the synchronization of the get. Shouldn't matter if it
        // doesn't exist.
        String foundPassword = null;
        User user = null;
        synchronized (this) {
            foundPassword = userPassword.get(username);
            if (foundPassword != null) {
                user = users.get(username);
            }
        }

        if (foundPassword == null || !foundPassword.equals(password)) {
            throw new UserManagerException("Username/Password not found.");
        }
        // Once it gets to this point, no exception has been thrown. User
        // shoudn't be
        // null, but adding this check for if user and user/password hash tables
        // go
        // out of sync.
        if (user == null) {
            throw new UserManagerException("Internal error: User not found.");
        }

        // Add all the roles the group has to the user
        resolveGroupRoles(groupRoles, user);
        user.setPermissions(new User.UserPermissions() {
            @Override
            public boolean hasPermission(String permission) {
                return true;
            }

            @Override
            public void addPermission(String permission) {
            }
        });
        return user;
    }
}
