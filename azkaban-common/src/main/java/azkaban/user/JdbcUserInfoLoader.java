package azkaban.user;

import azkaban.database.AbstractJdbcLoader;
import azkaban.utils.Props;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author lcs
 * @class JdbcUserInfoLoader
 * @date 2016/9/30.
 * @describe
 */
public class JdbcUserInfoLoader extends AbstractJdbcLoader implements UserInfoLoader {
    private static final Logger logger = Logger
            .getLogger(JdbcUserInfoLoader.class);
    private File tempDir;

    public JdbcUserInfoLoader(Props props) {
        super(props);
        tempDir = new File(props.getString("project.temp.dir", "temp"));
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }
    }

    @Override
    public List<User> fetchAllUser() throws UserManagerException {
        QueryRunner runner = createQueryRunner();
        FetchAllUserResultHandle fetchAllUserResultHandle = new FetchAllUserResultHandle();
        List<User> users = null;
        try {
            users = runner.query(FetchAllUserResultHandle.SELECT_ALL_USER, fetchAllUserResultHandle);
        } catch (SQLException e) {
            throw new UserManagerException("Error fetch all user", e);
        }
        return users;
    }

    private static class FetchAllUserResultHandle implements
            ResultSetHandler<List<User>> {
        private static String SELECT_ALL_USER =
                "select username,password_md5,email from user_info";

        @Override
        public List<User> handle(ResultSet rs) throws SQLException {
            if (!rs.next()) {
                return Collections.<User>emptyList();
            }
            List<User> users = new ArrayList<User>();
            do {
                String username = rs.getString(1);
                String password = rs.getString(2);
                String email = rs.getString(3);

                User user = new User(username);
                user.setPassword(password);
                if (!StringUtils.isBlank(email)) {
                    user.setEmail(email);
                }
                users.add(user);
            } while (rs.next());
            return users;
        }
    }

    @Override
    public List<Role> fetchAllRole() throws UserManagerException {
        QueryRunner runner = createQueryRunner();
        FetchAllRoleResultHandle fetchAllRoleResultHandle = new FetchAllRoleResultHandle();
        List<Role> roles = null;
        try {
            roles = runner.query(FetchAllRoleResultHandle.SELECT_ALL_ROLE, fetchAllRoleResultHandle);
        } catch (SQLException e) {
            throw new UserManagerException("Error fetch all role", e);
        }
        return roles;
    }

    private static class FetchAllRoleResultHandle implements
            ResultSetHandler<List<Role>> {
        private static String SELECT_ALL_ROLE =
                "select rolename,permission from role_info";

        @Override
        public List<Role> handle(ResultSet rs) throws SQLException {
            if (!rs.next()) {
                return Collections.<Role>emptyList();
            }
            List<Role> roles = new ArrayList<Role>();
            do {
                String name = rs.getString(1);
                String permissions = rs.getString(2);

                String[] permissionSplit = permissions.split("\\s*,\\s*");
                Permission perm = new Permission();
                for (String permString : permissionSplit) {
                    try {
                        Permission.Type type = Permission.Type.valueOf(permString);
                        perm.addPermission(type);
                    } catch (IllegalArgumentException e) {
                        logger.error("Error adding type " + permString
                                + ". Permission doesn't exist.", e);
                    }
                }
                Role role = new Role(name, perm);
                roles.add(role);
            } while (rs.next());
            return roles;
        }
    }

    @Override
    public List<Group> fetchAllGroup() throws UserManagerException {
        QueryRunner runner = createQueryRunner();
        FetchAllGroupResultHandle fetchAllGroupResultHandle = new FetchAllGroupResultHandle();
        List<Group> groups = null;
        try {
            groups = runner.query(FetchAllGroupResultHandle.SELECT_ALL_GROUP, fetchAllGroupResultHandle);
        } catch (SQLException e) {
            throw new UserManagerException("Error fetch all group", e);
        }
        return groups;
    }

    private static class FetchAllGroupResultHandle implements
            ResultSetHandler<List<Group>> {
        private static String SELECT_ALL_GROUP = "select distinct groupname from group_info";

        @Override
        public List<Group> handle(ResultSet rs) throws SQLException {
            if (!rs.next()) {
                return Collections.<Group>emptyList();
            }
            List<Group> groups = new ArrayList<Group>();
            do {
                String groupname = rs.getString(1);
                Group group = new Group();
                group.setGroupName(groupname);
                groups.add(group);
            } while (rs.next());
            return groups;
        }
    }

    @Override
    public List<UserRole> fetchAllUserRole() throws UserManagerException {
        QueryRunner runner = createQueryRunner();
        FetchAllUserRoleResultHandle fetchAllUserRoleResultHandle = new FetchAllUserRoleResultHandle();
        List<UserRole> userRoles = null;
        try {
            userRoles = runner.query(FetchAllUserRoleResultHandle.SELECT_ALL_ROLE_RESULT, fetchAllUserRoleResultHandle);
        } catch (SQLException e) {
            throw new UserManagerException("Error fetch all user role", e);
        }
        return userRoles;
    }

    private static class FetchAllUserRoleResultHandle implements
            ResultSetHandler<List<UserRole>> {
        private static String SELECT_ALL_ROLE_RESULT = "select distinct username,rolename from user_role t1 inner join user_info t2 on t1.userid=t2.id inner join role_info t3 on t1.roleid=t3.id";

        @Override
        public List<UserRole> handle(ResultSet rs) throws SQLException {
            if (!rs.next()) {
                return Collections.<UserRole>emptyList();
            }
            List<UserRole> userRoles = new ArrayList<UserRole>();
            do {
                String username = rs.getString(1);
                String rolename = rs.getString(2);
                UserRole userRole = new UserRole();
                userRole.setUserName(username);
                userRole.setRoleName(rolename);
                userRoles.add(userRole);
            } while (rs.next());
            return userRoles;
        }
    }

    @Override
    public List<UserGroup> fetchAllUserGroup() throws UserManagerException {
        QueryRunner runner = createQueryRunner();
        FetchAllUserGroupResultHandle fetchAllUserGroupResultHandle = new FetchAllUserGroupResultHandle();
        List<UserGroup> userGroups = null;
        try {
            userGroups = runner.query(FetchAllUserGroupResultHandle.SELECT_ALL_USER_GROUP, fetchAllUserGroupResultHandle);
        } catch (SQLException e) {
            throw new UserManagerException("Error fetch all user group", e);
        }
        return userGroups;
    }

    private static class FetchAllUserGroupResultHandle implements
            ResultSetHandler<List<UserGroup>> {
        private static String SELECT_ALL_USER_GROUP = "select distinct username,groupname from user_group t1 inner join user_info t2 on t1.userid=t2.id inner join group_info t3 on t1.groupid=t3.id";

        @Override
        public List<UserGroup> handle(ResultSet rs) throws SQLException {
            if (!rs.next()) {
                return Collections.<UserGroup>emptyList();
            }
            List<UserGroup> userGroups = new ArrayList<UserGroup>();
            do {
                String username = rs.getString(1);
                String groupname = rs.getString(2);
                UserGroup userGroup = new UserGroup();
                userGroup.setUserName(username);
                userGroup.setGroupName(groupname);

                userGroups.add(userGroup);
            } while (rs.next());
            return userGroups;
        }
    }

    @Override
    public List<UserProxy> fetchAllUserProxy() throws UserManagerException {
        QueryRunner runner = createQueryRunner();
        FetchAllUserProxyResultHandle fetchAllUserProxyResultHandle = new FetchAllUserProxyResultHandle();
        List<UserProxy> userProxys = null;
        try {
            userProxys = runner.query(FetchAllUserProxyResultHandle.SELECT_ALL_USER_PROXY, fetchAllUserProxyResultHandle);
        } catch (SQLException e) {
            throw new UserManagerException("Error fetch all user proxy", e);
        }
        return userProxys;
    }

    private static class FetchAllUserProxyResultHandle implements
            ResultSetHandler<List<UserProxy>> {
        private static String SELECT_ALL_USER_PROXY = "select distinct name1,name2 from \n" +
                "(select username name1 from user_proxy t1 inner join user_info t2 on t1.userid=t2.id) t1,\n" +
                "(select username name2 from user_proxy t1 inner join user_info t2 on t1.proxyuserid=t2.id) t2";

        @Override
        public List<UserProxy> handle(ResultSet rs) throws SQLException {
            if (!rs.next()) {
                return Collections.<UserProxy>emptyList();
            }
            List<UserProxy> userProxys = new ArrayList<UserProxy>();
            do {
                String username = rs.getString(1);
                String proxyuser = rs.getString(2);
                UserProxy userProxy = new UserProxy();
                userProxy.setUserName(username);
                userProxy.setProxyName(proxyuser);

                userProxys.add(userProxy);
            } while (rs.next());
            return userProxys;
        }
    }

    @Override
    public List<GroupRole> fetchAllGroupRole() throws UserManagerException {
        QueryRunner runner = createQueryRunner();
        FetchAllGroupRoleResultHandle fetchAllGroupRoleResultHandle = new FetchAllGroupRoleResultHandle();
        List<GroupRole> groupRoleS = null;
        try {
            groupRoleS = runner.query(FetchAllGroupRoleResultHandle.SELECT_ALL_GROUP_ROLE, fetchAllGroupRoleResultHandle);
        } catch (SQLException e) {
            throw new UserManagerException("Error fetch all group role", e);
        }
        return groupRoleS;
    }

    private static class FetchAllGroupRoleResultHandle implements
            ResultSetHandler<List<GroupRole>> {
        private static String SELECT_ALL_GROUP_ROLE = "select distinct t1.groupname,t2.rolename from group_info t1 inner join role_info t2 on t1.roleid=t2.id";

        @Override
        public List<GroupRole> handle(ResultSet rs) throws SQLException {
            if (!rs.next()) {
                return Collections.<GroupRole>emptyList();
            }
            List<GroupRole> groupRoles = new ArrayList<GroupRole>();
            do {
                String groupname = rs.getString(1);
                String rolename = rs.getString(2);
                GroupRole groupRole = new GroupRole();
                groupRole.setGroupName(groupname);
                groupRole.setRoleName(rolename);

                groupRoles.add(groupRole);
            } while (rs.next());
            return groupRoles;
        }
    }

    private Connection getConnection() throws UserManagerException {
        Connection connection = null;
        try {
            connection = super.getDBConnection(false);
        } catch (Exception e) {
            DbUtils.closeQuietly(connection);
            throw new UserManagerException("Error getting DB connection.", e);
        }

        return connection;
    }
}
