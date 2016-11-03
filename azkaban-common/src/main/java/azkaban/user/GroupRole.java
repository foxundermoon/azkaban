package azkaban.user;

/**
 * @author lcs
 * @class GroupRole
 * @date 2016/9/30.
 * @describe 组-角色
 */
public class GroupRole {
    private String groupName;
    private String roleName;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getRoleName() {
        return roleName;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }
}
