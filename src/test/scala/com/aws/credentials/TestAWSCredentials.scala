package com.aws.credentials

import com.utils.AppConfig
import org.junit.{Assert, Test}
import org.hamcrest.CoreMatchers._

class TestAWSCredentials {

  @Test
  def testDefaultAWSCredentialsProvider(): Unit = {
    Assert.assertThat(AppConfig.DEFAULT_AWS_PROFILE_NAME, is("default"))
    Assert.assertThat(AppConfig.DEFAULT_AWS_REGION_NAME, is("ap-northeast-2"))

    Assert.assertThat(CredentialsFactory.getCredentialsProvider.getCredentials != null, is(true))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testProfileCredentialsException(): Unit = {
    Assert.assertThat(CredentialsFactory.getCredentialsProvider("testProfile").getCredentials == null, is(true))
  }
}
