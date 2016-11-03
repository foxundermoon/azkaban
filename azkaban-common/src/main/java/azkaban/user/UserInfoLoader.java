package azkaban.user;

import java.util.List;

/**
 * @author lcs
 * @class UserInfoLoader
 * @date 2016/9/30.
 * @describe
 */
public interface UserInfoLoader {
    public List<User> fetchAllUser()throws UserManagerException;

    public List<Role> fetchAllRole()throws UserManagerException;

    public List<Group> fetchAllGroup()throws UserManagerException;

    public List<UserRole> fetchAllUserRole()throws UserManagerException;

    public List<UserGroup> fetchAllUserGroup()throws UserManagerException;

    public List<UserProxy> fetchAllUserProxy()throws UserManagerException;

    public List<GroupRole> fetchAllGroupRole()throws UserManagerException;

}
