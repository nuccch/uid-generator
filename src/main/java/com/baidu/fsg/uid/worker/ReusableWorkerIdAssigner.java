package com.baidu.fsg.uid.worker;

import com.baidu.fsg.uid.utils.DockerUtils;
import com.baidu.fsg.uid.utils.NetUtils;
import com.baidu.fsg.uid.worker.dao.WorkerNodeDAO;
import com.baidu.fsg.uid.worker.entity.WorkerNodeEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;

/**
 * 重复使用WorkerId
 * @author chench
 * @date 2024.06.19
 */
public class ReusableWorkerIdAssigner implements WorkerIdAssigner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReusableWorkerIdAssigner.class);

    @Resource
    private WorkerNodeDAO workerNodeDAO;

    /**
     * 根据IP地址和类型查询WorkerId，如果已经存在则返回，否则添加节点信息到数据库并返回WorkerId。
     *
     * @return 已经存在的WorkerId，或者新添加的WorkerId。
     */
    @Override
    public long assignWorkerId() {
        // query worker node entity
        String host = null;
        int type = -1;
        if (DockerUtils.isDocker()) {
            host = DockerUtils.getDockerHost();
            type = WorkerNodeType.CONTAINER.value();
        } else {
            host = NetUtils.getLocalAddress();
            type = WorkerNodeType.ACTUAL.value();
        }
        WorkerNodeEntity workerNodeEntity = workerNodeDAO.getWorkerNodeByHostType(host, type);
        if (workerNodeEntity != null) {
            LOGGER.info("Reuse exists worker id:{}, host:{}, type:{}", workerNodeEntity.getId(), host, type);
            return workerNodeEntity.getId();
        }

        // build worker node entity
        workerNodeEntity = buildWorkerNode();

        // add worker node for new (ignore the same IP + PORT)
        workerNodeDAO.addWorkerNode(workerNodeEntity);
        LOGGER.info("Add worker node:" + workerNodeEntity);

        return workerNodeEntity.getId();
    }
}