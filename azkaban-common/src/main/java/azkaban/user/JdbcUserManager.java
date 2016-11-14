package azkaban.user;

import azkaban.utils.Props;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author lcs
 * @class JdbcUserManager
 * @date 2016/9/30.
 * @describe 从mysql中读取用户相关数据
 */
public class JdbcUserManager implements UserManager {
    private static final Logger logger = Logger.getLogger(JdbcUserManager.class);
    private UserInfoLoader userInfoLoader = null;
    private HashMap<String, User> users;
    private HashMap<String, String> userPassword;
    private HashMap<String, Role> roles;
    private HashMap<String, Set<String>> groupRoles;
    private HashMap<String, Set<String>> proxyUserMap;
    public static final String LOAD_DATA_SLEEEP_TIME = "user.manager.load.data.sleep.time";


    public JdbcUserManager(Props props) {
        this.userInfoLoader = new JdbcUserInfoLoader(props);
        final long sleepTime = props.getLong(LOAD_DATA_SLEEEP_TIME);
        //每隔LOAD_DATA_SLEEEP_TIME时间去mysql中加载用户数据到map中
        Thread loadUserDataThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        initData();//初始化数据
                    } catch (Exception e) {
                        logger.error(" load user data error", e);
                    } finally {
                        try {
                            Thread.sleep(sleepTime);
                        } catch (Exception e) {
                            logger.error(" thread sleep error");
                        }
                    }
                }
            }
        }, "loadUserDataThread");
        loadUserDataThread.setDaemon(true);
        loadUserDataThread.start();
    }

    /**
     * 从mysql中初始化数据到内存
     */
    public void initData() {
        HashMap<String, User> users = new HashMap<String, User>();
        HashMap<String, String> userPassword = new HashMap<String, String>();
        HashMap<String, Role> roles = new HashMap<String, Role>();
        HashMap<String, Set<String>> groupRoles =
                new HashMap<String, Set<String>>();
        HashMap<String, Set<String>> proxyUserMap =
                new HashMap<String, Set<String>>();

        List<User> userList = null;
        List<Role> roleList = null;
        List<Group> groupList = null;
        List<UserRole> userRoleList = null;
        List<UserGroup> userGroupList = null;
        List<UserProxy> userProxyList = null;
        List<GroupRole> groupRoleList = null;
        try {
            userList = userInfoLoader.fetchAllUser();
        } catch (UserManagerException e) {
            throw new RuntimeException("Could not load users from store.", e);
        }
        try {
            roleList = userInfoLoader.fetchAllRole();
        } catch (UserManagerException e) {
            throw new RuntimeException("Could not load roles from store.", e);
        }
        try {
            groupList = userInfoLoader.fetchAllGroup();
        } catch (UserManagerException e) {
            throw new RuntimeException("Could not load groups from store.", e);
        }
        try {
            userRoleList = userInfoLoader.fetchAllUserRole();
        } catch (UserManagerException e) {
            throw new RuntimeException("Could not load user roles from store.", e);
        }
        try {
            userGroupList = userInfoLoader.fetchAllUserGroup();
        } catch (UserManagerException e) {
            throw new RuntimeException("Could not load user groups from store.", e);
        }
        try {
            userProxyList = userInfoLoader.fetchAllUserProxy();
        } catch (UserManagerException e) {
            throw new RuntimeException("Could not load user proxys from store.", e);
        }
        try {
            groupRoleList = userInfoLoader.fetchAllGroupRole();
        } catch (UserManagerException e) {
            throw new RuntimeException("Could not load group roles from store.", e);
        }
        //将数据加载到map中
        loadUserData(userList, userRoleList, userGroupList, userProxyList, users, userPassword, proxyUserMap);
        loadRoleData(roleList, roles);
        loadGroupData(groupList, groupRoleList, groupRoles);

        // Synchronize the swap. Similarly, the gets are synchronized to this.
        synchronized (this) {
            this.users = users;
            this.userPassword = userPassword;
            this.roles = roles;
            this.proxyUserMap = proxyUserMap;
            this.groupRoles = groupRoles;
        }
        logger.error(" initData method ,users.seize=" + users.size() + ",userPassword.seize=" + userPassword.size() + ",roles.seize=" + roles.size() + ",proxyUserMap.seize=" + proxyUserMap.size() + ",groupRoles.seize=" + groupRoles.size());
    }

    /**
     * 加载用户信息
     *
     * @param users
     * @param userPassword
     * @param proxyUserMap
     */
    public void loadUserData(List<User> userList, List<UserRole> userRoleList, List<UserGroup> userGroupList, List<UserProxy> userProxyList,
                             HashMap<String, User> users, HashMap<String, String> userPassword, HashMap<String, Set<String>> proxyUserMap) {
        if (userList == null) {
            throw new RuntimeException("Error loading user. The userList is null.");
        }
        User user = null;
        for (User userObj : userList) {
            user = userObj;
            userPassword.put(user.getUserId(), user.getPassword());//密码
            if (userRoleList != null) {
                for (UserRole userRole : userRoleList) {
                    if (user.getUserId().equals(userRole.getUserName())) {
                        user.addRole(userRole.getRoleName());//用户-角色
                    }
                }
            }
            if (userGroupList != null) {
                for (UserGroup userGroup : userGroupList) {
                    if (user.getUserId().equals(userGroup.getUserName())) {
                        user.addGroup(userGroup.getGroupName());//用户-组
                    }
                }
            }
            if (userProxyList != null) {
                for (UserProxy userProxy : userProxyList) {
                    if (user.getUserId().equals(userProxy.getUserName())) {
                        Set<String> proxySet = proxyUserMap.get(user.getUserId());
                        if (proxySet == null) {
                            proxySet = new HashSet<String>();
                            proxyUserMap.put(user.getUserId(), proxySet);//用户-代理
                        }
                        proxySet.add(userProxy.getProxyName());
                    }
                }
            }
            users.put(user.getUserId(), user);//用户
        }
    }

    /**
     * 加载角色信息
     *
     * @param roles
     */
    public void loadRoleData(List<Role> roleList, HashMap<String, Role> roles) {
        if (roleList == null) {
            throw new RuntimeException("Error loading role. The roleList is null.");
        }
        for (Role role : roleList) {
            String roleName = role.getName();
            roles.put(roleName, role);
        }
    }

    /**
     * 加载组-角色信息
     *
     * @param groupRoles
     */
    public void loadGroupData(List<Group> groupList, List<GroupRole> groupRoleList, HashMap<String, Set<String>> groupRoles) {
        if (groupRoleList == null) {
            throw new RuntimeException("Error loading groupRole. The groupRoleList is null.");
        }

        for (Group group : groupList) {
            String groupName = group.getGroupName();
            Set<String> roleSet = new HashSet<String>();
            for (GroupRole groupRole : groupRoleList) {
                if (groupName.equals(groupRole.getGroupName())) {
                    roleSet.add(groupRole.getRoleName());
                }
            }
            groupRoles.put(groupName, roleSet);
        }

    }

    ;

    @Override
    public User getUser(String username, String password) throws UserManagerException {
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
        resolveGroupRoles(user);
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

    private void resolveGroupRoles(User user) {
        for (String group : user.getGroups()) {
            Set<String> groupRoleSet = groupRoles.get(group);
            if (groupRoleSet != null) {
                for (String role : groupRoleSet) {
                    user.addRole(role);
                }
            }
        }
    }

    @Override
    public boolean validateUser(String username) {
        return users.containsKey(username);
    }

    @Override
    public boolean validateGroup(String group) {
        // Return true. Validation should be added when groups are added to the xml.
        return true;
    }

    @Override
    public Role getRole(String roleName) {
        return roles.get(roleName);
    }

    @Override
    public boolean validateProxyUser(String proxyUser, User realUser) {
        if (proxyUserMap.containsKey(realUser.getUserId())
                && proxyUserMap.get(realUser.getUserId()).contains(proxyUser)) {
            return true;
        } else {
            return false;
        }
    }
}
